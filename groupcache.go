/*
Copyright 2012 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package groupcache provides a data loading mechanism with caching
// and de-duplication that works across a set of peer processes.
//
// Each data Get first consults its local cache, otherwise delegates
// to the requested key's canonical owner, which then checks its cache
// or finally gets the data.  In the common case, many concurrent
// cache misses across a set of peers for the same key result in just
// one cache fill.
package groupcache

import (
	"errors"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"fmt"
	"strings"

	log "github.com/Sirupsen/logrus"
	pb "github.com/cyber4ron/groupcache/groupcachepb"
	"github.com/cyber4ron/groupcache/lru"
	"github.com/cyber4ron/groupcache/singleflight"
)

// A Getter loads data for a key.
type Getter interface {
	// Get returns the value identified by key, populating dest.
	//
	// The returned data must be unversioned. That is, key must
	// uniquely describe the loaded data, without an implicit
	// current time, and without relying on cache expiration
	// mechanisms.
	Get(ctx Context, key string, dest Sink) error

	GetBatch(ctx Context, kvs []*KeyValue) error
}

// leave GetterFunc for backward compatibility
type GetterFunc func(ctx Context, key string, dest Sink) error

type GetterBatchFunc func(ctx Context, kvs []*KeyValue) error

// GetterBatchFunc implements Getter
type GCGetter []interface{}

func (f GCGetter) Get(ctx Context, key string, dest Sink) error {
	if f[0] != nil {
		return f[0].(GetterFunc)(ctx, key, dest)
	} else {
		return errors.New("not implemented.")
	}
}

func (f GCGetter) GetBatch(ctx Context, kvs []*KeyValue) error {
	if len(f) >= 2 && f[1] != nil {
		return f[1].(GetterBatchFunc)(ctx, kvs)
	} else {
		return errors.New("not implemented.")
	}
}

const (
	defaultPipelineConcurrency = 16
	defaultPipelineCapacity = 10000
	defaultPipelineMGetTimeout = time.Second * 5
)

var (
	mu sync.RWMutex
	groups = make(map[string]*Group)

	initPeerServerOnce sync.Once
	initPeerServer     func()
)

// GetGroup returns the named group previously created with NewGroup, or
// nil if there's no such group.
func GetGroup(name string) *Group {
	mu.RLock()
	g := groups[name]
	mu.RUnlock()
	return g
}

// NewGroup creates a coordinated group-aware Getter from a Getter.
//
// The returned Getter tries (but does not guarantee) to run only one
// Get call at once for a given key across an entire set of peer
// processes. Concurrent callers both in the local process and in
// other processes receive copies of the answer once the original Get
// completes.
//
// The group name must be unique for each getter.
func NewGroup(name string, cacheBytes int64, getter Getter, process ProcessFunc) *Group {
	return newGroup(name, cacheBytes, getter, nil, process)
}

func NewGroupOpts(name string, cacheBytes int64, getter Getter, process ProcessFunc, opt *GroupOpts) *Group {
	return newGroupOpts(name, cacheBytes, getter, nil, process, opt)
}

// If peers is nil, the peerPicker is called via a sync.Once to initialize it.
func newGroup(name string, cacheBytes int64, getter Getter, peers PeerPicker, process ProcessFunc) *Group {
	return newGroupOpts(name, cacheBytes, getter, peers, process, nil)
}

func newGroupOpts(name string, cacheBytes int64, getter Getter, peers PeerPicker, process ProcessFunc, opt *GroupOpts) *Group {
	if getter == nil {
		panic("nil Getter")
	}
	if process == nil {
		process = defaultProcessFunc
	}
	mu.Lock()
	defer mu.Unlock()
	initPeerServerOnce.Do(callInitPeerServer)
	if _, dup := groups[name]; dup {
		panic("duplicate registration of group " + name)
	}
	g := &Group{
		name:       name,
		getter:     getter,
		peers:      peers,
		cacheBytes: cacheBytes,
		loadGroup:  &singleflight.Group{},
	}

	if opt != nil {
		g.opt = *opt
	}
	if g.opt.PipelineConcurrency == 0 {
		g.opt.PipelineConcurrency = defaultPipelineConcurrency
	}
	if g.opt.PipelineCapacity == 0 {
		g.opt.PipelineCapacity = defaultPipelineCapacity
	}
	if g.opt.PipelineMGetTimeout == 0 {
		g.opt.PipelineMGetTimeout = defaultPipelineMGetTimeout
	}
	g.pipeline = &Pipeline{
		Concurrency: g.opt.PipelineConcurrency,
		Capacity:    g.opt.PipelineCapacity,
		MGetTimeout: g.opt.PipelineMGetTimeout,
		Group:             g,
		Process:     process,
	}
	g.pipeline.Start()

	if fn := newGroupHook; fn != nil {
		fn(g)
	}
	groups[name] = g
	return g
}

// newGroupHook, if non-nil, is called right after a new group is created.
var newGroupHook func(*Group)

// RegisterNewGroupHook registers a hook that is run each time
// a group is created.
func RegisterNewGroupHook(fn func(*Group)) {
	if newGroupHook != nil {
		panic("RegisterNewGroupHook called more than once")
	}
	newGroupHook = fn
}

// RegisterServerStart registers a hook that is run when the first
// group is created.
func RegisterServerStart(fn func()) {
	if initPeerServer != nil {
		panic("RegisterServerStart called more than once")
	}
	initPeerServer = fn
}

func callInitPeerServer() {
	if initPeerServer != nil {
		initPeerServer()
	}
}

// A Group is a cache namespace and associated data loaded spread over
// a group of 1 or more machines.
type Group struct {
	name        string
	getter      Getter
	peersOnce   sync.Once
	peers       PeerPicker
	cacheBytes  int64 // limit for sum of mainCache and hotCache size

			  // mainCache is a cache of the keys for which this process
			  // (amongst its peers) is authoritative. That is, this cache
			  // contains keys which consistent hash on to this process's
			  // peer number.
	mainCache   cache

			  // hotCache contains keys/values for which this peer is not
			  // authoritative (otherwise they would be in mainCache), but
			  // are popular enough to warrant mirroring in this process to
			  // avoid going over the network to fetch from a peer.  Having
			  // a hotCache avoids network hotspotting, where a peer's
			  // network card could become the bottleneck on a popular key.
			  // This cache is used sparingly to maximize the total number
			  // of key/value pairs that can be stored globally.
	hotCache    cache

			  // loadGroup ensures that each key is only fetched once
			  // (either locally or remotely), regardless of the number of
			  // concurrent callers.
	loadGroup   flightGroup

	_           int32 // force Stats to be 8-byte aligned on 32-bit platforms

			  // Stats are statistics on the group.
	Stats       Stats

			  // For expiration functionality.
	expiration  time.Duration
	loadTimeout time.Duration

			  // pipeline for mget
	pipeline    *Pipeline

	opt         GroupOpts
}

type GroupOpts struct {
	PipelineConcurrency int
	PipelineCapacity    int
	PipelineMGetTimeout time.Duration
}

// flightGroup is defined as an interface which flightgroup.Group
// satisfies.  We define this so that we may test with an alternate
// implementation.
type flightGroup interface {
	// Done is called when Do is done.
	Do(key string, fn func() (interface{}, error)) (interface{}, error)
}

// Stats are per-group statistics.
type Stats struct {
	Gets               AtomicInt // any Get request, including from peers
	CacheHits          AtomicInt // either cache was good
	PeerLoads          AtomicInt // either remote load or remote cache hit (not an error)
	PeerErrors         AtomicInt
	Loads              AtomicInt // (gets - cacheHits)
	LoadsDeduped       AtomicInt // after singleflight
	LocalLoads         AtomicInt // total good local loads
	LocalLoadErrs      AtomicInt // total bad local loads
	ServerRequests     AtomicInt // gets that came over the network from peers
	Expires            AtomicInt // times of request expired object
	LoadsDedupedExpire AtomicInt // times of reload expired object
	Cleanups           AtomicInt // times of cleaning expired object
}

// Name returns the name of the group.
func (g *Group) Name() string {
	return g.name
}

func (g *Group) GetMaxCacheSize() int64 {
	return g.cacheBytes
}

func (g *Group) initPeers() {
	if g.peers == nil {
		g.peers = getPeers(g.name)
	}
}

func (g *Group) Get(ctx Context, key string, dest Sink) error {
	g.peersOnce.Do(g.initPeers)
	g.Stats.Gets.Add(1)
	if dest == nil {
		return errors.New("groupcache: nil dest Sink")
	}
	value, which, cacheHit := g.lookupCache(key)

	if cacheHit {
		g.Stats.CacheHits.Add(1)
		if g.expiration > 0 {
			return g.handleExpiration(ctx, key, dest, value, which)
		} else {
			return setSinkView(dest, value)
		}
	}

	_, err := g.loadAndPopulate(ctx, key, dest, false)
	return err
}

type KeyValue struct {
	Key        string
	Idx        int32
	Data       []byte   // result data
	Dest       Sink
	Err        error

	whichRead  CacheType
	whichWrite CacheType
	value      ByteView // for temporarily holding data
}

func NewKeyValue(key string, idx int32) *KeyValue {
	kv := &KeyValue{
		Key: key,
		Idx: idx,
		whichRead: None,
		whichWrite: None,
	}

	kv.Data = []byte{}
	kv.Dest = AllocatingByteSliceSink(&kv.Data)
	return kv
}

func (g *Group) GetBatch(ctx Context, kvs []*KeyValue) error {
	g.peersOnce.Do(g.initPeers)
	g.Stats.Gets.Add(int64(len(kvs)))

	// partition keys
	KVsHit := []*KeyValue{}
	KvsMiss := []*KeyValue{}
	for _, kv := range kvs {
		value, which, cacheHit := g.lookupCache(kv.Key)
		if cacheHit {
			kv.value = value
			kv.whichRead = which
			KVsHit = append(KVsHit, kv)
		} else {
			KvsMiss = append(KvsMiss, kv)
		}
	}

	// handle hit kvs
	KvsExpired := []*KeyValue{}
	if len(KVsHit) != 0 {
		g.Stats.CacheHits.Add(int64(len(KVsHit)))
		if g.expiration > 0 {
			var err error
			KvsExpired, err = g.checkExpirationBatch(ctx, KVsHit);
			if err != nil {
				log.Errorln("[GetBatch] handleExpirationBatch failed, err:", err)
			}
		} else {
			for _, kv := range KVsHit {
				if err := setSinkView(kv.Dest, kv.value); err != nil {
					log.Errorln("[GetBatch] setSinkView failed, err:", err)
					kv.Err = fmt.Errorf("[GetBatch] setSinkView failed, err: %s", err.Error())
				}
			}
		}
	}

	// load kvs
	KvsToLoad := append(KvsMiss, KvsExpired...)
	if len(KvsToLoad) != 0 {
		if err := g.loadBatch(ctx, KvsToLoad); err != nil {
			log.Errorln("[GetBatch] loadBatch failed, err:", err)
		} else {
			g.handleExpiredKeys(KvsExpired)
		}
	}

	return nil
}

func (g *Group) Put(ctx Context, key string, value []byte, timeOffsetSec int64) error {
	g.peersOnce.Do(g.initPeers)
	packedBytes, err := packTimestamp(value, GetUnixTime() + timeOffsetSec)
	if err != nil {
		return err
	}

	if peer, ok := g.peers.PickPeer(key); ok {
		req := &pb.PutRequest{
			Group: &g.name,
			Key:   &key,
			Value: packedBytes,
		}
		resp := &pb.PutResponse{}
		err := peer.Put(nil, req, resp)
		if err != nil {
			return err
		}
		log.Debugf("====> put to peer, succ, key: %s, value len: %v", key, len(packedBytes))
		if rand.Intn(10) == 0 {
			g.populateCache(key, ByteView{b: packedBytes}, &g.hotCache)
		}
	} else {
		log.Debugf("====> putting to local, key: %s, value len: %v", key, len(packedBytes))
		g.populateCache(key, ByteView{b: packedBytes}, &g.mainCache)
	}

	return nil
}

func (g *Group) PutBatch(ctx Context, keys []string, values [][]byte, timeOffsetSecs []int64) error {
	g.peersOnce.Do(g.initPeers)

	// sanity check
	if len(keys) != len(values) || len(values) != len(timeOffsetSecs) {
		log.Errorln("[PutBatch] len(keys) != len(values) || len(values) != len(timeOffsetSecs)")
		return errors.New("[PutBatch] len(keys) != len(values) || len(values) != len(timeOffsetSecs)")
	}

	// pack timestamps
	for i, value := range values {
		packedBytes, err := packTimestamp(value, GetUnixTime() + timeOffsetSecs[i])
		if err != nil {
			log.Errorln("pack timestamp failed, err:", err)
			continue
		}
		values[i] = packedBytes
	}

	// partition keys
	peerKeys := map[ProtoGetter][]string{}
	peerValues := map[ProtoGetter][][]byte{}
	localKeys := []string{}
	localValues := [][]byte{}
	for i, key := range keys {
		peer, ok := g.peers.PickPeer(key)
		if !ok {
			localKeys = append(localKeys, key)
			localValues = append(localValues, values[i])
		} else {
			if _, ok := peerKeys[peer]; !ok {
				peerKeys[peer] = []string{}
				peerValues[peer] = [][]byte{}
			}

			peerKeys[peer] = append(peerKeys[peer], key)
			peerValues[peer] = append(peerValues[peer], values[i])
		}
	}

	// put to peers, todo: goroutine
	for peer, keys := range peerKeys {
		if len(keys) != len(peerValues[peer]) {
			return errors.New("[PutBatch] len(keys) != len(peerValues[peer])")
		}
		req := &pb.PutBatchRequest{
			Group: &g.name,
			Keys:   keys,
			Values: peerValues[peer],
		}
		resp := &pb.PutBatchResponse{}
		if err := peer.PutBatch(nil, req, resp); err != nil {
			return err
		}
		log.Debugf("====> put to peer, succ, keys: %v", keys)
		for i, key := range keys {
			if rand.Intn(10) == 0 {
				g.populateCache(key, ByteView{b: peerValues[peer][i]}, &g.hotCache)
			}
		}
	}

	// put to local
	for i, key := range localKeys {
		g.populateCache(key, ByteView{b: localValues[i]}, &g.mainCache)
	}

	return nil
}

// loadAndPopulate loads the key and populates dest
func (g *Group) loadAndPopulate(ctx Context, key string, dest Sink, expired bool) (CacheType, error) {
	destPopulated := false
	value, which, destPopulated, err := g.load(ctx, key, dest, expired)
	if err != nil {
		return None, err
	}
	if destPopulated {
		return which, nil
	}
	return None, setSinkView(dest, value)
}

// load loads key either by invoking the getter locally or by sending it to another machine.
// which tracks which cache the load function write.
func (g *Group) load(ctx Context, key string, dest Sink, expiredCheck bool) (value ByteView, which CacheType, destPopulated bool, err error) {
	g.Stats.Loads.Add(1)
	viewI, err := g.loadGroup.Do(key, func() (interface{}, error) {
		which = None
		if expiredCheck {
			g.Stats.LoadsDedupedExpire.Add(1)
		}
		if value, _, cacheHit := g.lookupCache(key); cacheHit {
			writeTs, err := getTimestampByteView(value)
			if err != nil {
				return nil, err
			}
			if GetUnixTime() - writeTs - int64(g.expiration.Seconds()) < 0 {
				g.Stats.CacheHits.Add(1)
				return value, nil
			}
		}
		g.Stats.LoadsDeduped.Add(1)
		var value ByteView
		var err error

		// try to get from peer if authorized by peer
		if peer, ok := g.peers.PickPeer(key); ok {
			value, which, err = g.getFromPeer(ctx, peer, key)
			if err == nil {
				g.Stats.PeerLoads.Add(1)
				return value, nil
			}
			log.Infof("peer error: %v", err)
			g.Stats.PeerErrors.Add(1)
		}

		// authorized by local host or peer failed
		value, err = g.getLocally(ctx, key, dest)
		if err != nil {
			g.Stats.LocalLoadErrs.Add(1)
			return nil, err
		}
		g.Stats.LocalLoads.Add(1)

		// only one caller of load gets this return value
		destPopulated = true
		g.populateCache(key, value, &g.mainCache)
		which = MainCache
		return value, nil
	})
	if err == nil {
		value = viewI.(ByteView)
	}
	return
}

// loadBatch loads keys either by invoking the getter locally or by sending them to another machine.
// currently called when keys are missed or expired.
func (g *Group) loadBatch(ctx Context, kvs []*KeyValue) (err error) {
	g.Stats.Loads.Add(int64(len(kvs)))

	// partition keys
	kvsPeer := map[ProtoGetter][]*KeyValue{}
	kvsLocal := []*KeyValue{}
	for _, kv := range kvs {
		if peer, ok := g.peers.PickPeer(kv.Key); ok {
			if _, ok := kvsPeer[peer]; !ok {
				kvsPeer[peer] = []*KeyValue{}
			}
			kvsPeer[peer] = append(kvsPeer[peer], kv)
		} else {
			kvsLocal = append(kvsLocal, kv)
		}
	}

	// get from peers
	cnt := 0
	var wg sync.WaitGroup
	wg.Add(len(kvsPeer))
	kvsFailedPar := make([][]*KeyValue, len(kvsPeer))

	for peer, peerKvs := range kvsPeer {
		go func(idx int, kvs []*KeyValue) {
			kvsFailedPar[idx] = []*KeyValue{}
			err := g.getFromPeerBatch(ctx, peer, kvs, time.Millisecond * 50)
			if err != nil {
				log.Errorf("[loadBatch] peer error: %v", err)
				g.Stats.PeerErrors.Add(int64(len(kvs)))
				kvsFailedPar[idx] = append(kvsFailedPar[idx], kvs...)
			} else {
				for _, kv := range kvs {
					if kv.Err != nil {
						log.Infof("[loadBatch] peer kv error: %v", kv.Err.Error())
						g.Stats.PeerErrors.Add(1)
						kvsFailedPar[idx] = append(kvsFailedPar[idx], kv)
					}
				}
			}
			wg.Done()
		}(cnt, peerKvs)
		cnt++
	}
	wg.Wait()

	kvsFailed := []*KeyValue{}
	for _, kvs := range kvsFailedPar {
		kvsFailed = append(kvsFailed, kvs...)
	}

	// reset Err field of failed keys
	for _, kv := range kvsFailed {
		kv.Err = nil
	}

	// combine failed keys to local keys and get from local
	kvsLocal = append(kvsLocal, kvsFailed...);
	if len(kvsLocal) > 0 {
		err = g.getter.GetBatch(ctx, kvsLocal)
		if err != nil {
			g.Stats.LocalLoadErrs.Add(int64((len(kvsLocal))))
			log.Errorln("[loadBatch] GetBatch failed, err:", err)
			errNew := fmt.Errorf("[loadBatch] GetBatch failed, err: %s", err.Error())
			for _, kv := range kvs {
				kv.Err = errNew
			}
			return errNew
		} else {
			// populate local keys to main cache
			g.Stats.LocalLoads.Add(int64(len(kvsLocal)))
			for _, kv := range kvsLocal {
				if kv.Err != nil {
					log.Infoln("[loadBatch] locally get failed, err:", kv.Err.Error())
					continue
				}
				view, _ := kv.Dest.view()
				g.populateCache(kv.Key, view, &g.mainCache)
				kv.whichWrite = MainCache
			}
		}
	}

	return
}

func (g *Group) getLocally(ctx Context, key string, dest Sink) (ByteView, error) {
	err := g.getter.Get(ctx, key, dest)
	if err != nil {
		return ByteView{}, err
	}
	return dest.view()
}

// CacheType tracks which cache the object write to. if err != nil, CacheType should be None
func (g *Group) getFromPeer(ctx Context, peer ProtoGetter, key string) (ByteView, CacheType, error) {
	req := &pb.GetRequest{
		Group: &g.name,
		Key:   &key,
	}
	res := &pb.GetResponse{}
	ts := time.Now()
	err := peer.Get(ctx, req, res)
	if err != nil {
		return ByteView{}, None, err
	}
	if elapse := time.Since(ts); elapse > time.Millisecond * 50 {
		log.Warnf("[groupcache] getFromPeer slow, key: %v, time: %v", key, elapse)
	}
	value := ByteView{b: res.Value}
	if rand.Intn(10) == 0 {
		g.populateCache(key, value, &g.hotCache)
		return value, HotCache, nil
	}
	return value, None, nil
}

func (g *Group) getFromPeerBatch(ctx Context, peer ProtoGetter, kvs []*KeyValue, timeout time.Duration) error {
	if len(kvs) == 0 {
		return nil
	}

	kvsPb := make([]*pb.KeyValue, len(kvs))
	for i, kv := range kvs {
		idx := (int32)(i)
		kvsPb[i] = &pb.KeyValue{
			Key: &kv.Key,
			Idx: &idx,
		}
	}
	req := &pb.GetBatchRequest{
		Group:      &g.name,
		Kvs:        kvsPb,
	}
	resp := &pb.GetBatchResponse{}

	// get from peer. todo: load in background?
	errCh := make(chan error)
	go func() {
		errCh <- peer.GetBatch(ctx, req, resp)
	}()
	select {
	case err := <-errCh:
		if err != nil {
			errNew := fmt.Errorf("peer.GetBatch failed, err: %s", err.Error())
			for _, kv := range kvs {
				kv.Err = errNew
			}
			return errNew
		}

		// read response value
		for i, respKv := range resp.Kvs {
			if respKv.Error != nil && *(respKv.Error) != "" {
				kvs[i].Err = errors.New(*respKv.Error)
				continue
			}
			setSinkView(kvs[i].Dest, ByteView{b: respKv.Value})
			if rand.Intn(10) == 0 {
				g.populateCache(*respKv.Key, ByteView{b: respKv.Value}, &g.hotCache)
				kvs[i].whichWrite = HotCache
			}
		}

	case <-timeProvider.After(timeout):
		errNew := fmt.Errorf("peer.GetBatch timeout(%v).", timeout)
		for _, kv := range kvs {
			kv.Err = errNew
		}
		return errNew
	}

	return nil
}

func (g *Group) lookupCache(key string) (value ByteView, which CacheType, ok bool) {
	which = None
	if g.cacheBytes <= 0 {
		return
	}
	value, ok = g.mainCache.get(key)
	if ok {
		which = MainCache
		return
	}
	value, ok = g.hotCache.get(key)
	if ok {
		which = HotCache
		return
	}
	return
}

func (g *Group) populateCache(key string, value ByteView, cache *cache) {
	if g.cacheBytes <= 0 {
		return
	}
	cache.add(key, value)

	// Evict items from cache(s) if necessary.
	for {
		mainBytes := g.mainCache.bytes()
		hotBytes := g.hotCache.bytes()
		if mainBytes + hotBytes <= g.cacheBytes {
			return
		}

		victim := &g.mainCache
		if hotBytes > mainBytes / 8 {
			victim = &g.hotCache
		}
		victim.removeOldest()
	}
}

// CacheType represents a type of cache.
type CacheType int

const (
	// The MainCache is the cache for items that this peer is the
	// owner for.
	MainCache CacheType = iota + 1

	// The HotCache is the cache for items that seem popular
	// enough to replicate to this node, even though it's not the
	// owner.
	HotCache

	// None means none of above.
	None
)

// CacheStats returns stats about the provided cache within the group.
func (g *Group) CacheStats(which CacheType) CacheStats {
	switch which {
	case MainCache:
		return g.mainCache.stats()
	case HotCache:
		return g.hotCache.stats()
	default:
		return CacheStats{}
	}
}

// cache is a wrapper around an *lru.Cache that adds synchronization,
// makes values always be ByteView, and counts the size of all keys and
// values.
type cache struct {
	mu         sync.RWMutex
	nbytes     int64 // of all keys and values
	lru        *lru.Cache
	nhit, nget int64
	nevict     int64 // number of evictions
}

func (c *cache) stats() CacheStats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return CacheStats{
		Bytes:     c.nbytes,
		Items:     c.itemsLocked(),
		Gets:      c.nget,
		Hits:      c.nhit,
		Evictions: c.nevict,
	}
}

func (c *cache) add(key string, value ByteView) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lru == nil {
		c.lru = &lru.Cache{
			OnEvicted: func(key lru.Key, value interface{}) {
				val := value.(ByteView)
				c.nbytes -= int64(len(key.(string))) + int64(val.Len())
				c.nevict++
			},
		}
	}
	// remove dup key
	if c.lru.Contains(key) {
		c.lru.Remove(key)
	}
	c.lru.Add(key, value)
	c.nbytes += int64(len(key)) + int64(value.Len())
}

func (c *cache) get(key string) (value ByteView, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nget++
	if c.lru == nil {
		return
	}
	vi, ok := c.lru.Get(key)
	if !ok {
		return
	}
	c.nhit++
	return vi.(ByteView), true
}

func (c *cache) remove(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lru != nil {
		c.lru.Remove(key)
	}
}

func (c *cache) removeOldest() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lru != nil {
		c.lru.RemoveOldest()
	}
}

func (c *cache) bytes() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.nbytes
}

func (c *cache) items() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.itemsLocked()
}

func (c *cache) itemsLocked() int64 {
	if c.lru == nil {
		return 0
	}
	return int64(c.lru.Len())
}

// An AtomicInt is an int64 to be accessed atomically.
type AtomicInt int64

// Add atomically adds n to i.
func (i *AtomicInt) Add(n int64) {
	atomic.AddInt64((*int64)(i), n)
}

// Get atomically gets the value of i.
func (i *AtomicInt) Get() int64 {
	return atomic.LoadInt64((*int64)(i))
}

func (i *AtomicInt) String() string {
	return strconv.FormatInt(i.Get(), 10)
}

// CacheStats are returned by stats accessors on Group.
type CacheStats struct {
	Bytes     int64
	Items     int64
	Gets      int64
	Hits      int64
	Evictions int64
}

// for debug
func printKV(msg string, kvs []*KeyValue) {
	if log.GetLevel() == log.DebugLevel {
		strs := []string{}
		for _, kv := range kvs {
			strs = append(strs, fmt.Sprintf("%+v", kv))
		}
		log.Debugf("%s %+v", msg, strings.Join(strs, ", "))
	}
}

// for debug
func printKVPB(msg string, kvs []*pb.KeyValue) {
	if log.GetLevel() == log.DebugLevel {
		strs := []string{}
		for _, kv := range kvs {
			strs = append(strs, fmt.Sprintf("%+v", kv))
		}
		log.Debugf("%s %+v", msg, strings.Join(strs, ", "))
	}
}

// for debug
func printKVMap(msg string, kvsMap map[ProtoGetter][]*KeyValue) {
	if log.GetLevel() == log.DebugLevel {
		mapStr := []string{}
		for peer, kvs := range kvsMap {
			kvStr := []string{}
			for _, kv := range kvs {
				kvStr = append(kvStr, fmt.Sprintf("%+v", kv))
			}
			mapStr = append(mapStr, fmt.Sprintf("{%v: %+v}", peer.(*httpGetter).baseURL, strings.Join(kvStr, ",")))
		}
		log.Debugf("%s, %s", msg, strings.Join(mapStr, ", "))
	}
}