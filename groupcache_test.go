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

// Tests for groupcache.

package groupcache

import (
	"errors"
	"fmt"
	"hash/crc32"
	"math/rand"
	"reflect"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/golang/protobuf/proto"

	pb "github.com/cyber4ron/groupcache/groupcachepb"
	testpb "github.com/cyber4ron/groupcache/testpb"
	"strconv"
	"github.com/Sirupsen/logrus"
	"strings"
	"net/http"
	"os"
	"os/exec"
	"flag"
)

var (
	once sync.Once
	stringGroup, protoGroup Getter

	stringc = make(chan string)

	dummyCtx Context

	// cacheFills is the number of times stringGroup or
	// protoGroup's Getter have been called. Read using the
	// cacheFills function.
	cacheFills AtomicInt
)

const (
	stringGroupName = "string-group"
	protoGroupName = "proto-group"
	testMessageType = "google3/net/groupcache/go/test_proto.TestMessage"
	fromChan = "from-chan"
	cacheSize = 1 << 20
)

func startAsPeer(level logrus.Level, expirationTime time.Duration) {
	logrus.SetLevel(level)

	logrus.Infoln("peerAddrs:", peerAddrs)
	addrs := strings.Split(*peerAddrs, ",")

	p := NewHTTPPoolOpts("http://" + addrs[*peerIndex], &HTTPPoolOptions{
		BasePath: "/_groupcache/",
		ConfigOpts: ConfigOpts{"127.0.0.1:2181", "/common", time.Second},
	})
	p.Set(addrToURL(addrs)...)

	getter := GCGetter([]interface{}{GetterFunc(func(ctx Context, key string, dest Sink) error {
		dest.SetTimestampBytes([]byte(strconv.Itoa(*peerIndex) + ":" + key), GetUnixTime())
		return nil
	}), GetterBatchFunc(func(ctx Context, kvs []*KeyValue) error {
		if rand.Intn(100) == 0 {
			return errors.New("injected batch error.")
		}
		printKV("--> kvs: %v", kvs)
		for i, kv := range kvs {
			if rand.Intn(100) == 0 {
				kv.Err = errors.New("injected kv error.")
				continue
			}
			kv.Dest.SetTimestampBytes([]byte(strconv.Itoa(*peerIndex) + ":" + kv.Key), GetUnixTime())
			view, _ := kv.Dest.view()
			logrus.Debugf("i: %v, view.String(): %v", i, view.String())
		}
		return nil
	})})
	g := NewGroupOpts("getBatchTest", 1 << 10, getter, nil, &GroupOpts{PipelineConcurrency: 4})
	g.SetExpiration(expirationTime)

	http.Handle(p.opts.BasePath, p)
	logrus.Fatal(http.ListenAndServe(addrs[*peerIndex], nil))
}

func testSetup() {
	stringGroup = NewGroup(stringGroupName, cacheSize, GCGetter([]interface{}{func(_ Context, key string, dest Sink) error {
		if key == fromChan {
			key = <-stringc
		}
		cacheFills.Add(1)
		return dest.SetString("ECHO:" + key)
	}}), nil)

	protoGroup = NewGroup(protoGroupName, cacheSize, GCGetter([]interface{}{func(_ Context, key string, dest Sink) error {
		if key == fromChan {
			key = <-stringc
		}
		cacheFills.Add(1)
		return dest.SetProto(&testpb.TestMessage{
			Name: proto.String("ECHO:" + key),
			City: proto.String("SOME-CITY"),
		})
	}}), nil)
}

// tests that a Getter's Get method is only called once with two
// outstanding callers.  This is the string variant.
func TestGetDupSuppressString(t *testing.T) {
	once.Do(testSetup)
	// Start two getters. The first should block (waiting reading
	// from stringc) and the second should latch on to the first
	// one.
	resc := make(chan string, 2)
	for i := 0; i < 2; i++ {
		go func() {
			var s string
			if err := stringGroup.Get(dummyCtx, fromChan, StringSink(&s)); err != nil {
				resc <- "ERROR:" + err.Error()
				return
			}
			resc <- s
		}()
	}

	// Wait a bit so both goroutines get merged together via
	// singleflight.
	// TODO(bradfitz): decide whether there are any non-offensive
	// debug/test hooks that could be added to singleflight to
	// make a sleep here unnecessary.
	time.Sleep(250 * time.Millisecond)

	// Unblock the first getter, which should unblock the second
	// as well.
	stringc <- "foo"

	for i := 0; i < 2; i++ {
		select {
		case v := <-resc:
			if v != "ECHO:foo" {
				t.Errorf("got %q; want %q", v, "ECHO:foo")
			}
		case <-time.After(5 * time.Second):
			t.Errorf("timeout waiting on getter #%d of 2", i + 1)
		}
	}
}

// tests that a Getter's Get method is only called once with two
// outstanding callers.  This is the proto variant.
func TestGetDupSuppressProto(t *testing.T) {
	once.Do(testSetup)
	// Start two getters. The first should block (waiting reading
	// from stringc) and the second should latch on to the first
	// one.
	resc := make(chan *testpb.TestMessage, 2)
	for i := 0; i < 2; i++ {
		go func() {
			tm := new(testpb.TestMessage)
			if err := protoGroup.Get(dummyCtx, fromChan, ProtoSink(tm)); err != nil {
				tm.Name = proto.String("ERROR:" + err.Error())
			}
			resc <- tm
		}()
	}

	// Wait a bit so both goroutines get merged together via
	// singleflight.
	// TODO(bradfitz): decide whether there are any non-offensive
	// debug/test hooks that could be added to singleflight to
	// make a sleep here unnecessary.
	time.Sleep(250 * time.Millisecond)

	// Unblock the first getter, which should unblock the second
	// as well.
	stringc <- "Fluffy"
	want := &testpb.TestMessage{
		Name: proto.String("ECHO:Fluffy"),
		City: proto.String("SOME-CITY"),
	}
	for i := 0; i < 2; i++ {
		select {
		case v := <-resc:
			if !reflect.DeepEqual(v, want) {
				t.Errorf(" Got: %v\nWant: %v", proto.CompactTextString(v), proto.CompactTextString(want))
			}
		case <-time.After(5 * time.Second):
			t.Errorf("timeout waiting on getter #%d of 2", i + 1)
		}
	}
}

func countFills(f func()) int64 {
	fills0 := cacheFills.Get()
	f()
	return cacheFills.Get() - fills0
}

func TestCaching(t *testing.T) {
	once.Do(testSetup)
	fills := countFills(func() {
		for i := 0; i < 10; i++ {
			var s string
			if err := stringGroup.Get(dummyCtx, "TestCaching-key", StringSink(&s)); err != nil {
				t.Fatal(err)
			}
		}
	})
	if fills != 1 {
		t.Errorf("expected 1 cache fill; got %d", fills)
	}
}

func TestCacheEviction(t *testing.T) {
	once.Do(testSetup)
	testKey := "TestCacheEviction-key"
	getTestKey := func() {
		var res string
		for i := 0; i < 10; i++ {
			if err := stringGroup.Get(dummyCtx, testKey, StringSink(&res)); err != nil {
				t.Fatal(err)
			}
		}
	}
	fills := countFills(getTestKey)
	if fills != 1 {
		t.Fatalf("expected 1 cache fill; got %d", fills)
	}

	g := stringGroup.(*Group)
	evict0 := g.mainCache.nevict

	// Trash the cache with other keys.
	var bytesFlooded int64
	// cacheSize/len(testKey) is approximate
	for bytesFlooded < cacheSize + 1024 {
		var res string
		key := fmt.Sprintf("dummy-key-%d", bytesFlooded)
		stringGroup.Get(dummyCtx, key, StringSink(&res))
		bytesFlooded += int64(len(key) + len(res))
	}
	evicts := g.mainCache.nevict - evict0
	if evicts <= 0 {
		t.Errorf("evicts = %v; want more than 0", evicts)
	}

	// Test that the key is gone.
	fills = countFills(getTestKey)
	if fills != 1 {
		t.Fatalf("expected 1 cache fill after cache trashing; got %d", fills)
	}
}

type fakePeer struct {
	hits int
	fail bool
}

func (p *fakePeer) Get(_ Context, in *pb.GetRequest, out *pb.GetResponse) error {
	p.hits++
	if p.fail {
		return errors.New("simulated error from peer")
	}
	out.Value = []byte("got:" + in.GetKey())
	return nil
}

func (p *fakePeer) GetBatch(_ Context, in *pb.GetBatchRequest, out *pb.GetBatchResponse) error {
	return nil
}

func (p *fakePeer) Put(_ Context, in *pb.PutRequest, out *pb.PutResponse) error {
	return nil
}

func (p *fakePeer) PutBatch(_ Context, in *pb.PutBatchRequest, out *pb.PutBatchResponse) error {
	return nil
}

type fakePeers []ProtoGetter

func (fakePeers) RegisterAndWatch() error {
	return errors.New("no implementation")
}

func (p fakePeers) PickPeer(key string) (peer ProtoGetter, ok bool) {
	if len(p) == 0 {
		return
	}
	n := crc32.Checksum([]byte(key), crc32.IEEETable) % uint32(len(p))
	return p[n], p[n] != nil
}

// tests that peers (virtual, in-process) are hit, and how much.
func TestPeers(t *testing.T) {
	once.Do(testSetup)
	rand.Seed(123)
	peer0 := &fakePeer{}
	peer1 := &fakePeer{}
	peer2 := &fakePeer{}
	peerList := fakePeers([]ProtoGetter{peer0, peer1, peer2, nil})
	const cacheSize = 0 // disabled
	localHits := 0
	getter := func(_ Context, key string, dest Sink) error {
		localHits++
		return dest.SetString("got:" + key)
	}
	testGroup := newGroup("TestPeers-group", cacheSize, GCGetter([]interface{}{getter, nil}), peerList, nil)
	run := func(name string, n int, wantSummary string) {
		// Reset counters
		localHits = 0
		for _, p := range []*fakePeer{peer0, peer1, peer2} {
			p.hits = 0
		}

		for i := 0; i < n; i++ {
			key := fmt.Sprintf("key-%d", i)
			want := "got:" + key
			var got string
			err := testGroup.Get(dummyCtx, key, StringSink(&got))
			if err != nil {
				t.Errorf("%s: error on key %q: %v", name, key, err)
				continue
			}
			if got != want {
				t.Errorf("%s: for key %q, got %q; want %q", name, key, got, want)
			}
		}
		summary := func() string {
			return fmt.Sprintf("localHits = %d, peers = %d %d %d", localHits, peer0.hits, peer1.hits, peer2.hits)
		}
		if got := summary(); got != wantSummary {
			t.Errorf("%s: got %q; want %q", name, got, wantSummary)
		}
	}
	resetCacheSize := func(maxBytes int64) {
		g := testGroup
		g.cacheBytes = maxBytes
		g.mainCache = cache{}
		g.hotCache = cache{}
	}

	// Base case; peers all up, with no problems.
	resetCacheSize(1 << 20)
	run("base", 200, "localHits = 49, peers = 51 49 51")

	// Verify cache was hit.  All localHits are gone, and some of
	// the peer hits (the ones randomly selected to be maybe hot)
	run("cached_base", 200, "localHits = 0, peers = 49 47 48")
	resetCacheSize(0)

	// With one of the peers being down.
	// TODO(bradfitz): on a peer number being unavailable, the
	// consistent hashing should maybe keep trying others to
	// spread the load out. Currently it fails back to local
	// execution if the first consistent-hash slot is unavailable.
	peerList[0] = nil
	run("one_peer_down", 200, "localHits = 100, peers = 0 49 51")

	// Failing peer
	peerList[0] = peer0
	peer0.fail = true
	run("peer0_failing", 200, "localHits = 100, peers = 51 49 51")
}

func TestTruncatingByteSliceTarget(t *testing.T) {
	once.Do(testSetup)

	var buf [100]byte
	s := buf[:]
	if err := stringGroup.Get(dummyCtx, "short", TruncatingByteSliceSink(&s)); err != nil {
		t.Fatal(err)
	}
	if want := "ECHO:short"; string(s) != want {
		t.Errorf("short key got %q; want %q", s, want)
	}

	s = buf[:6]
	if err := stringGroup.Get(dummyCtx, "truncated", TruncatingByteSliceSink(&s)); err != nil {
		t.Fatal(err)
	}
	if want := "ECHO:t"; string(s) != want {
		t.Errorf("truncated key got %q; want %q", s, want)
	}
}

func TestAllocatingByteSliceTarget(t *testing.T) {
	var dst []byte
	sink := AllocatingByteSliceSink(&dst)

	inBytes := []byte("some bytes")
	sink.SetBytes(inBytes)
	if want := "some bytes"; string(dst) != want {
		t.Errorf("SetBytes resulted in %q; want %q", dst, want)
	}
	v, err := sink.view()
	if err != nil {
		t.Fatalf("view after SetBytes failed: %v", err)
	}
	if &inBytes[0] == &dst[0] {
		t.Error("inBytes and dst share memory")
	}
	if &inBytes[0] == &v.b[0] {
		t.Error("inBytes and view share memory")
	}
	if &dst[0] == &v.b[0] {
		t.Error("dst and view share memory")
	}
}

// orderedFlightGroup allows the caller to force the schedule of when
// orig.Do will be called.  This is useful to serialize calls such
// that singleflight cannot dedup them.
type orderedFlightGroup struct {
	mu     sync.Mutex
	stage1 chan bool
	stage2 chan bool
	orig   flightGroup
}

func (g *orderedFlightGroup) Do(key string, fn func() (interface{}, error)) (interface{}, error) {
	<-g.stage1
	<-g.stage2
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.orig.Do(key, fn)
}

// TestNoDedup tests invariants on the cache size when singleflight is
// unable to dedup calls.
func TestNoDedup(t *testing.T) {
	const testkey = "testkey"
	const testval = "testval"
	ts := GetUnixTime()
	packedBytes, _ := packTimestamp([]byte(testval), ts)
	g := newGroup("testgroup", 1024, GCGetter([]interface{}{func(_ Context, key string, dest Sink) error {
		return dest.SetBytes(packedBytes)
	}}), nil, nil)

	orderedGroup := &orderedFlightGroup{
		stage1: make(chan bool),
		stage2: make(chan bool),
		orig:   g.loadGroup,
	}
	// Replace loadGroup with our wrapper so we can control when
	// loadGroup.Do is entered for each concurrent request.
	g.loadGroup = orderedGroup

	// Issue two idential requests concurrently.  Since the cache is
	// empty, it will miss.  Both will enter load(), but we will only
	// allow one at a time to enter singleflight.Do, so the callback
	// function will be called twice.
	resc := make(chan string, 2)
	for i := 0; i < 2; i++ {
		go func() {
			var s string
			if err := g.Get(dummyCtx, testkey, StringSink(&s)); err != nil {
				resc <- "ERROR:" + err.Error()
				return
			}
			resc <- s
		}()
	}

	// Ensure both goroutines have entered the Do routine.  This implies
	// both concurrent requests have checked the cache, found it empty,
	// and called load().
	orderedGroup.stage1 <- true
	orderedGroup.stage1 <- true
	orderedGroup.stage2 <- true
	orderedGroup.stage2 <- true

	for i := 0; i < 2; i++ {
		if s := <-resc; s != string(packedBytes) {
			t.Errorf("result is %s want %s", s, testval)
		}
	}

	const wantItems = 1
	if g.mainCache.items() != wantItems {
		t.Errorf("mainCache has %d items, want %d", g.mainCache.items(), wantItems)
	}

	fmt.Printf("stat: %+v\n", g.Stats)
	fmt.Printf("hot cache: %+v\n", g.hotCache.stats())
	fmt.Printf("main cache: %+v\n", g.mainCache.stats())

	// If the singleflight callback doesn't double-check the cache again
	// upon entry, we would increment nbytes twice but the entry would
	// only be in the cache once.
	wantBytes := int64(len(testkey) + len(packedBytes))
	if g.mainCache.nbytes != wantBytes {
		t.Errorf("cache has %d bytes, want %d", g.mainCache.nbytes, wantBytes)
	}
}

func TestGroupStatsAlignment(t *testing.T) {
	var g Group
	off := unsafe.Offsetof(g.Stats)
	if off % 8 != 0 {
		t.Fatal("Stats structure is not 8-byte aligned.")
	}
}

func TestPut(t *testing.T) {
	getter := GCGetter([]interface{}{func(ctx Context, key string, dest Sink) error {
		dest.SetString(strconv.Itoa(*peerIndex) + ":" + key)
		return nil
	}})

	g := NewGroup("putTest", 1 << 10, getter, nil)
	err := g.Put(nil, "001", []byte("x"), 2000)
	if err != nil {
		fmt.Println(err)
	}

	var value string
	err = g.Get(nil, "001", StringSink(&value))
	if err != nil {
		fmt.Println(err)
	}

	r, ts, _ := UnpackTimestamp([]byte(value))
	fmt.Printf("r: %v, ts: %v, now: %v\n", string(r), ts, time.Now().Unix())
}

// todo: 测cache未命中，命中，过期；从peer节点get，集群变更。检查返回的数据，cache状态。maincache中key的数量
func TestGroup_GetBatchPeer(t *testing.T) {
	logrus.SetLevel(logrus.DebugLevel)

	flag.Parse()

	expirationTime := time.Second * 2
	if *peerChild {
		startAsPeer(logrus.DebugLevel, expirationTime)
		os.Exit(0)
	}

	// const nPeers = 0
	const nPeers = 24

	var allAddr []string
	for i := 0; i < nPeers + 1; i++ {
		allAddr = append(allAddr, pickFreeAddr(t))
	}

	logrus.Infoln("allAddr:", allAddr)

	var cmds []*exec.Cmd
	var wg sync.WaitGroup
	for i := 0; i < nPeers; i++ {
		cmd := exec.Command(os.Args[0],
			"--test.run=TestGroup_GetBatchPeer",
			"--test_peer_child",
			"--test_peer_addrs=" + strings.Join(allAddr, ","),
			"--test_peer_index=" + strconv.Itoa(i),
		)
		cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
		cmds = append(cmds, cmd)
		wg.Add(1)
		if err := cmd.Start(); err != nil {
			t.Fatal("failed to start child process: ", err)
		}
		go awaitAddrReady(t, allAddr[i], &wg)
	}
	defer func() {
		for i := 0; i < nPeers; i++ {
			if cmds[i].Process != nil {
				cmds[i].Process.Kill()
			}
		}
	}()
	wg.Wait()

	p := NewHTTPPoolOpts("http://" + allAddr[nPeers], &HTTPPoolOptions{
		BasePath: "/_groupcache/",
		ConfigOpts: ConfigOpts{"127.0.0.1:2181", "/common", time.Second},
	})
	p.Set(addrToURL(allAddr)...)

	getter := GCGetter([]interface{}{GetterFunc(func(ctx Context, key string, dest Sink) error {
		dest.SetTimestampBytes([]byte(strconv.Itoa(*peerIndex) + ":" + key), GetUnixTime())
		return nil
	}), GetterBatchFunc(func(ctx Context, kvs []*KeyValue) error {
		if rand.Intn(100) == 0 {
			return errors.New("injected batch error.")
		}
		printKV("--> kvs: %v", kvs)
		for i, kv := range kvs {
			if rand.Intn(100) == 0 {
				kv.Err = errors.New("injected kv error.")
				continue
			}
			kv.Dest.SetTimestampBytes([]byte(strconv.Itoa(*peerIndex) + ":" + kv.Key), GetUnixTime())
			view, _ := kv.Dest.view()
			logrus.Debugf("i: %v, view.String(): %v", i, view.String())
		}
		return nil
	})})
	g := NewGroupOpts("getBatchTest", 1 << 20, getter, nil, &GroupOpts{PipelineConcurrency: 4})
	g.SetExpiration(expirationTime)

	// test hit / expired / miss

	// cache miss
	kvs := []*KeyValue{}
	for i := range make([]byte, 100) {
		// 0 - 9
		kvs = append(kvs, NewKeyValue(fmt.Sprintf("%.3d", i), int32(i)))
	}
	err := g.GetBatch(nil, kvs)
	if err != nil {
		fmt.Println(err)
		t.Error(err)
	}

	printKV("---> cache hit", kvs)
	for i, kv := range kvs {
		if kv.Err != nil {
			logrus.Errorln("kv err, err:", kv.Err)
			continue
		} else {
			view, _ := kv.Dest.view()
			r, _, _ := UnpackTimestamp([]byte(view.ByteSlice()))
			fmt.Printf("1, r: %v\n", string(r))
			verifyString(t, fmt.Sprintf("%.3d", i), string(r)[strings.Index(string(r), ":") + 1:])
		}
	}

	// cache partial hit
	kvs = []*KeyValue{}
	for i := range make([]byte, 100) {
		// 5 - 14
		kvs = append(kvs, NewKeyValue(fmt.Sprintf("%.3d", i + 50), int32(i)))
	}
	err = g.GetBatch(nil, kvs)
	if err != nil {
		fmt.Println(err)
		t.Error(err)
	}

	printKV("---> cache partial hit", kvs)
	for i, kv := range kvs {
		if kv.Err != nil {
			logrus.Errorln("kv err, err:", kv.Err)
			continue
		} else {
			view, _ := kv.Dest.view()
			r, _, _ := UnpackTimestamp([]byte(view.ByteSlice()))
			fmt.Printf("2, r: %v\n", string(r))
			verifyString(t, fmt.Sprintf("%.3d", i + 50), string(r)[strings.Index(string(r), ":") + 1:])
		}
	}

	 // cache expired
	 time.Sleep(time.Second * 3)
	 kvs = []*KeyValue{}
	 for i := range make([]byte, 50) {
	 	// 0 - 4
	 	kvs = append(kvs, NewKeyValue(fmt.Sprintf("%.3d", i), int32(i)))
	 }
	 err = g.GetBatch(nil, kvs)
	 if err != nil {
	 	fmt.Println(err)
	 	t.Error(err)
	 }
	 printKV("---> cache expired", kvs)
	 for i, kv := range kvs {
	 	if kv.Err != nil {
	 		logrus.Errorln("kv err, err:", kv.Err)
	 		continue
	 	} else {
	 		view, _ := kv.Dest.view()
	 		r, ts, _ := UnpackTimestamp([]byte(view.ByteSlice()))
	 		fmt.Printf("3, r: %v, ts: %v\n", string(r), ts)
			if time.Now().Unix() - ts > expirationTime.Nanoseconds() / 1E9 {
				t.Errorf("expired check error, ts: %v, time.Now().Unix():%v", ts, time.Now().Unix())
			}
	 		verifyString(t, fmt.Sprintf("%.3d", i), string(r)[strings.Index(string(r), ":") + 1:])
	 	}
	 }

	// cache partial expired / miss / hit
	time.Sleep(time.Second * 1)
	kvs = []*KeyValue{}
	for i := range make([]byte, 50) {
		// 0 - 4
		kvs = append(kvs, NewKeyValue(fmt.Sprintf("%.3d", i), int32(i)))
	}
	err = g.GetBatch(nil, kvs)
	if err != nil {
		fmt.Println(err)
		t.Error(err)
	}

	kvs = []*KeyValue{}
	for i := range make([]byte, 250) {
		// 0 - 24
		kvs = append(kvs, NewKeyValue(fmt.Sprintf("%.3d", i), int32(i)))
	}
	err = g.GetBatch(nil, kvs)
	if err != nil {
		fmt.Println(err)
		t.Error(err)
	}
	printKV("---> cache partial expired / miss / hit", kvs)
	for _, kv := range kvs {
		if kv.Err != nil {
			logrus.Errorln("kv err, err:", kv.Err)
			continue
		} else {
			view, _ := kv.Dest.view()
			r, ts, _ := UnpackTimestamp([]byte(view.ByteSlice()))
			fmt.Printf("4, r: %v, ts: %v\n", string(r), ts)
			if time.Now().Unix() - ts > expirationTime.Nanoseconds() / 1E9 {
				t.Errorf("expired check error, ts: %v, time.Now().Unix():%v", ts, time.Now().Unix())
			}
			verifyString(t, kv.Key, string(r)[strings.Index(string(r), ":") + 1:])
		}
	}

	// cache stats
	fmt.Printf("main cache statas: %+v\n", g.mainCache.stats())
	fmt.Printf("hot cache statas: %+v\n", g.hotCache.stats())
}

func TestGroup_GetBatchPeerStress(t *testing.T) {
	logrus.SetLevel(logrus.WarnLevel)

	flag.Parse()

	expirationTime := time.Second * 4
	if *peerChild {
		startAsPeer(logrus.WarnLevel, expirationTime)
		os.Exit(0)
	}

	const nPeers = 48

	var allAddr []string
	for i := 0; i < nPeers + 1; i++ {
		allAddr = append(allAddr, pickFreeAddr(t))
	}

	logrus.Infoln("allAddr:", allAddr)

	var cmds []*exec.Cmd
	var wg sync.WaitGroup
	for i := 0; i < nPeers; i++ {
		cmd := exec.Command(os.Args[0],
			"--test.run=TestGroup_GetBatchPeerStress",
			"--test_peer_child",
			"--test_peer_addrs=" + strings.Join(allAddr, ","),
			"--test_peer_index=" + strconv.Itoa(i),
		)
		cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
		cmds = append(cmds, cmd)
		wg.Add(1)
		if err := cmd.Start(); err != nil {
			t.Fatal("failed to start child process: ", err)
		}
		go awaitAddrReady(t, allAddr[i], &wg)
	}
	defer func() {
		for i := 0; i < nPeers; i++ {
			if cmds[i].Process != nil {
				cmds[i].Process.Kill()
			}
		}
	}()
	wg.Wait()

	p := NewHTTPPoolOpts("http://" + allAddr[nPeers], &HTTPPoolOptions{
		BasePath: "/_groupcache/",
		ConfigOpts: ConfigOpts{"127.0.0.1:2181", "/common", time.Second},
	})
	p.Set(addrToURL(allAddr)...)

	getter := GCGetter([]interface{}{func(ctx Context, key string, dest Sink) error {
		dest.SetTimestampBytes([]byte(strconv.Itoa(*peerIndex) + ":" + key), GetUnixTime())
		return nil
	}, func(ctx Context, kvs []*KeyValue) error {
		if rand.Intn(100) == 0 {
			return errors.New("injected batch error.")
		}
		printKV("--> kvs", kvs)
		for i, kv := range kvs {
			if rand.Intn(100) == 0 {
				kv.Err = errors.New("injected kv error.")
				continue
			}
			kv.Dest.SetTimestampBytes([]byte(strconv.Itoa(*peerIndex) + ":" + kv.Key), GetUnixTime())
			view, _ := kv.Dest.view()
			logrus.Debugf("i: %v, view.String(): %v", i, view.String())
		}
		return nil
	}})
	g := NewGroupOpts("getBatchTest", 1 << 30, getter, nil, &GroupOpts{PipelineConcurrency: 4})
	g.SetExpiration(expirationTime)

	conc := 48
	times := 100
	batch := 100
	keyRange := int(1E5)

	ts := time.Now()
	wg.Add(conc)
	for c := 0; c < conc; c++ {
		go func() {
			for k := 0; k < times; k++ {
				kvs := []*KeyValue{}
				for i := 0; i < batch; i++ {
					r := rand.Intn(keyRange)
					kvs = append(kvs, &KeyValue{
						Key: fmt.Sprintf("%.8d", r),
						Idx: int32(i),
						Dest: AllocatingByteSliceSink(&[]byte{}),
					})
				}
				err := g.GetBatch(nil, kvs)
				if err != nil {
					fmt.Println(err)
					continue
				}
				printKV("---> cache partial expired / miss / hit", kvs)
				for _, kv := range kvs {
					view, _ := kv.Dest.view()
					r, ts, _ := UnpackTimestamp([]byte(view.ByteSlice()))
					if kv.Err != nil {
						logrus.Errorln("kv err, err:", kv.Err)
						continue
					}
					// 可能会多出几秒，+8
					if time.Now().Unix() - ts > expirationTime.Nanoseconds() / 1E9 + 8 {
						t.Errorf("expired check error, ts: %v, time.Now().Unix():%v", ts, time.Now().Unix())
					}
					verifyString(t, kv.Key, string(r)[strings.Index(string(r), ":") + 1:])
				}
			}
			wg.Done()
		}()
	}

	wg.Wait()
	fmt.Printf("elapse, %v", time.Since(ts))
	fmt.Printf("main cache statas: %+v\n", g.mainCache.stats())
	fmt.Printf("hot cache statas: %+v\n", g.hotCache.stats())
}


// TODO(bradfitz): port the Google-internal full integration test into here,
// using HTTP requests instead of our RPC system.
