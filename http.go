/*
Copyright 2013 Google Inc.

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

package groupcache

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"net"
	"time"

	"github.com/cyber4ron/groupcache/consistenthash"
	pb "github.com/cyber4ron/groupcache/groupcachepb"
	"github.com/golang/protobuf/proto"
	"github.com/Sirupsen/logrus"
)

const defaultBasePath = "/_groupcache/"

const defaultReplicas = 50

// HTTPPool implements PeerPicker for a pool of HTTP peers.
type HTTPPool struct {
					     // Context optionally specifies a context for the server to use when it
					     // receives a request.
					     // If nil, the server uses a nil Context.
	Context       func(*http.Request) Context

					     // Transport optionally specifies an http.RoundTripper for the client
					     // to use when it makes a request.
					     // If nil, the client uses http.DefaultTransport.
	Transport     func(Context) http.RoundTripper

					     // this peer's base URL, e.g. "https://example.net:8000"
	self          string

					     // opts specifies the options.
	opts          HTTPPoolOptions

	mu            sync.Mutex             // guards peers and httpGetters
	peers         *consistenthash.Map
	httpGetters   map[string]*httpGetter // keyed by e.g. "http://10.0.0.2:8008"

					     // configWatcher watches config changes in zk
	configWatcher *ConfigWatcher
}

// HTTPPoolOptions are the configurations of a HTTPPool.
type HTTPPoolOptions struct {
	// BasePath specifies the HTTP path that will serve groupcache requests.
	// If blank, it defaults to "/_groupcache/".
	BasePath string

	// Replicas specifies the number of key replicas on the consistent hash.
	// If blank, it defaults to 50.
	Replicas int

	// HashFn specifies the hash function of the consistent hash.
	// If blank, it defaults to crc32.ChecksumIEEE.
	HashFn   consistenthash.Hash

	// configurations of a ConfigWatcher
	ConfigOpts
}

// NewHTTPPool initializes an HTTP pool of peers, and registers itself as a PeerPicker.
// For convenience, it also registers itself as an http.Handler with http.DefaultServeMux.
// The self argument should be a valid base URL that points to the current server,
// for example "http://example.net:8000".
func NewHTTPPool(self string) *HTTPPool {
	p := NewHTTPPoolOpts(self, nil)
	http.Handle(p.opts.BasePath, p)
	return p
}

var httpPoolMade bool

func GetLocalIP() []string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return []string{}
	}
	ips := []string{}
	for _, address := range addrs {
		if ipNet, ok := address.(*net.IPNet); ok && !ipNet.IP.IsLoopback() {
			if ipNet.IP.To4() != nil {
				ips = append(ips, ipNet.IP.String())
			}
		}
	}
	return ips
}

// NewHTTPPoolOpts initializes an HTTP pool of peers with the given options.
// Unlike NewHTTPPool, this function does not register the created pool as an HTTP handler.
// The returned *HTTPPool implements http.Handler and must be registered using http.Handle.
func NewHTTPPoolOpts(self string, o *HTTPPoolOptions) *HTTPPool {
	if httpPoolMade {
		panic("groupcache: NewHTTPPool must be called only once")
	}
	httpPoolMade = true

	p := &HTTPPool{
		self:        self,
		httpGetters: make(map[string]*httpGetter),
	}
	if o != nil {
		p.opts = *o
	}
	if p.opts.BasePath == "" {
		p.opts.BasePath = defaultBasePath
	}
	if p.opts.Replicas == 0 {
		p.opts.Replicas = defaultReplicas
	}
	p.peers = consistenthash.New(p.opts.Replicas, p.opts.HashFn)

	watcher, err := NewConfigWatcher(p.opts.ZkServers, p.opts.ParentZNode, p.opts.SessionTimeout)
	if err != nil {
		panic(err)
	}
	p.configWatcher = watcher

	RegisterPeerPicker(func() PeerPicker {
		return p
	})

	return p
}

// Set updates the pool's list of peers.
// Each peer value should be a valid base URL,
// for example "http://example.net:8000".
func (p *HTTPPool) Set(peers ...string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.peers = consistenthash.New(p.opts.Replicas, p.opts.HashFn)
	p.peers.Add(peers...)
	p.httpGetters = make(map[string]*httpGetter, len(peers))
	for _, peer := range peers {
		p.httpGetters[peer] = &httpGetter{transport: p.Transport, baseURL: peer + p.opts.BasePath}
	}
}

func (p *HTTPPool) PickPeer(key string) (ProtoGetter, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.peers.IsEmpty() {
		return nil, false
	}
	if peer := p.peers.Get(key); peer != p.self {
		return p.httpGetters[peer], true
	}
	return nil, false
}

func (p *HTTPPool) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Parse request.
	if !strings.HasPrefix(r.URL.Path, p.opts.BasePath) {
		panic("HTTPPool serving unexpected path: " + r.URL.Path)
	}
	parts := strings.SplitN(r.URL.Path[len(p.opts.BasePath):], "/", 2)
	if len(parts) != 2 {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	groupName := parts[0]
	key, err := url.QueryUnescape(parts[1])
	if err != nil {
		http.Error(w, "decoding key: " + err.Error(), http.StatusBadRequest)
		return
	}

	switch strings.ToUpper(r.Method) {
	case "GET":
		// Fetch the value for this group/key.
		group := GetGroup(groupName)
		if group == nil {
			http.Error(w, "no such group: " + groupName, http.StatusNotFound)
			return
		}
		var ctx Context
		if p.Context != nil {
			ctx = p.Context(r)
		}

		group.Stats.ServerRequests.Add(1)
		var value []byte
		err = group.Get(ctx, key, AllocatingByteSliceSink(&value))
		if err != nil {
			logrus.Debugf("====> group.Get failed, key: %s, group: %s, err: %s", groupName, key, err.Error())
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// Write the value to the response body as a proto message.
		body, err := proto.Marshal(&pb.GetResponse{Value: value})
		if err != nil {
			logrus.Debugf("====> proto.Marshal failed, key: %s, group: %s, err: %s", groupName, key, err.Error())
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.Write(body)

	// Note: BATCH_GET returns Status_OK when part of the batch request failed
	case "BATCH_GET":
		group := GetGroup(groupName)
		if group == nil {
			http.Error(w, "no such group: " + groupName, http.StatusNotFound)
			return
		}
		var ctx Context
		if p.Context != nil {
			ctx = p.Context(r)
		}

		// construct kvs by req
		defer r.Body.Close()
		b := bufferPool.Get().(*bytes.Buffer)
		b.Reset()
		defer bufferPool.Put(b)
		_, err = io.Copy(b, r.Body)

		var req pb.GetBatchRequest
		if err := proto.Unmarshal(b.Bytes(), &req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
		}

		group.Stats.ServerRequests.Add(int64(len(req.Kvs)))

		kvs := make([]*KeyValue, len(req.Kvs))
		for i, kv := range req.Kvs {
			kvs[i] = NewKeyValue(*kv.Key, *kv.Idx)
		}
		printKV("BATCH_GET", kvs)

		// get values
		err = group.GetBatch(ctx, kvs)
		if err != nil {
			logrus.Debugf("====> group.GetBatch failed, keys: %v, group: %s, err: %s", groupName, kvs, err.Error())
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// construct kvsPb by kvs
		kvsPb := make([]*pb.KeyValue, len(kvs))
		for i, kv := range kvs {
			kvsPb[i] = &pb.KeyValue{
				Key: &kv.Key,
				Idx: &kv.Idx,
			}
			if kv.Err != nil && kv.Err.Error() != "" {
				s := kv.Err.Error()
				kvsPb[i].Error = &s
				continue
			}
			view, _ := kv.Dest.view()
			kvsPb[i].Value = view.ByteSlice()
		}

		// marshal and response
		printKVPB("BATCH_GET2:", kvsPb)
		body, err := proto.Marshal(&pb.GetBatchResponse{Kvs: kvsPb})
		if err != nil {
			logrus.Debugf("====> proto.Marshal failed, key: %s, group: %s, err: %s", groupName, key, err.Error())
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.Write(body)

	// single put
	case "PUT":
		logrus.Debugf("====> putting key, key: %s, group: %s", key, groupName)

		group := GetGroup(groupName)
		if group == nil {
			http.Error(w, "no such group: " + groupName, http.StatusNotFound)
			return
		}
		group.Stats.ServerRequests.Add(1)

		defer r.Body.Close()
		b := bufferPool.Get().(*bytes.Buffer)
		b.Reset()
		defer bufferPool.Put(b)
		_, err = io.Copy(b, r.Body)

		group.populateCache(key, ByteView{b: b.Bytes()}, &group.mainCache)
		w.WriteHeader(http.StatusOK)

	// batch put
	case "BATCH_PUT":
		group := GetGroup(groupName)
		if group == nil {
			http.Error(w, "no such group: " + groupName, http.StatusNotFound)
			return
		}
		group.Stats.ServerRequests.Add(1)

		defer r.Body.Close()
		b := bufferPool.Get().(*bytes.Buffer)
		b.Reset()
		defer bufferPool.Put(b)
		_, err = io.Copy(b, r.Body)

		var entries pb.Entries
		if err := proto.Unmarshal(b.Bytes(), &entries); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
		}

		if len(entries.Keys) != len(entries.Values) {
			http.Error(w, "batch put, len(keys) != len(values)", http.StatusBadRequest)
			return
		}
		logrus.Debugf("====> putting keys, keys: %v, group: %s", entries.Keys, groupName)

		for i, key := range entries.Keys {
			group.populateCache(key, ByteView{b: entries.Values[i]}, &group.mainCache)
		}
		w.WriteHeader(http.StatusOK)
	}
}

type httpGetter struct {
	transport func(Context) http.RoundTripper
	baseURL   string
}

var bufferPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

var httpTransport http.RoundTripper = &http.Transport{
	Proxy: http.ProxyFromEnvironment,
	DialContext: (&net.Dialer{
		Timeout:   1 * time.Second,
		KeepAlive: 30 * time.Second,
	}).DialContext,
	MaxIdleConns:          512,
	IdleConnTimeout:       180 * time.Second,
	TLSHandshakeTimeout:   10 * time.Second,
	ExpectContinueTimeout: 1 * time.Second,
}

func (h *httpGetter) Get(context Context, in *pb.GetRequest, out *pb.GetResponse) error {
	u := fmt.Sprintf(
		"%v%v/%v",
		h.baseURL,
		url.QueryEscape(in.GetGroup()),
		url.QueryEscape(in.GetKey()),
	)
	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return err
	}
	tr := httpTransport
	if h.transport != nil {
		tr = h.transport(context)
	}
	res, err := tr.RoundTrip(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("server returned: %v", res.Status)
	}
	b := bufferPool.Get().(*bytes.Buffer)
	b.Reset()
	defer bufferPool.Put(b)
	_, err = io.Copy(b, res.Body)
	if err != nil {
		return fmt.Errorf("reading response body: %v", err)
	}
	err = proto.Unmarshal(b.Bytes(), out)
	if err != nil {
		return fmt.Errorf("decoding response body: %v", err)
	}
	return nil
}

func (h *httpGetter) GetBatch(context Context, in *pb.GetBatchRequest, out *pb.GetBatchResponse) error {
	kvs := in.GetKvs()
	if len(kvs) == 0 {
		out = &pb.GetBatchResponse{Kvs: []*pb.KeyValue{}}
		return nil
	}

	u := fmt.Sprintf(
		"%v%v/%v",
		h.baseURL,
		url.QueryEscape(in.GetGroup()),
		"_",
	)
	data, err := proto.Marshal(in)
	req, err := http.NewRequest("BATCH_GET", u, bytes.NewBuffer(data))
	if err != nil {
		return fmt.Errorf("[GetBatch] http.NewRequest failed, err: %s", err.Error())
	}
	tr := httpTransport
	if h.transport != nil {
		tr = h.transport(context)
	}
	resp, err := tr.RoundTrip(req)
	if err != nil {
		return fmt.Errorf("[GetBatch] RoundTrip failed, err: %s", err.Error())
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("server returned: %v", resp.Status)
	}

	b := bufferPool.Get().(*bytes.Buffer)
	b.Reset()
	defer bufferPool.Put(b)
	_, err = io.Copy(b, resp.Body)
	if err != nil {
		return fmt.Errorf("reading response body: %v", err)
	}

	err = proto.Unmarshal(b.Bytes(), out)
	logrus.Debugf("out: %+v", out)
	if err != nil {
		return fmt.Errorf("decoding response body: %v", err)
	}

	return nil
}

func (h *httpGetter) Put(context Context, in *pb.PutRequest, out *pb.PutResponse) error {
	u := fmt.Sprintf(
		"%v%v/%v",
		h.baseURL,
		url.QueryEscape(in.GetGroup()),
		url.QueryEscape(in.GetKey()),
	)
	req, err := http.NewRequest("PUT", u, bytes.NewBuffer(in.GetValue()))
	if err != nil {
		return err
	}
	tr := httpTransport
	if h.transport != nil {
		tr = h.transport(context)
	}
	res, err := tr.RoundTrip(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("server returned: %v", res.Status)
	}
	return nil
}

func (h *httpGetter) PutBatch(context Context, in *pb.PutBatchRequest, out *pb.PutBatchResponse) error {
	u := fmt.Sprintf(
		"%v%v/%v",
		h.baseURL,
		url.QueryEscape(in.GetGroup()),
		url.QueryEscape("_"), // placeholder for key
	)

	entries := pb.Entries{Keys: in.Keys, Values: in.Values}
	data, err := proto.Marshal(&entries)
	if err != nil {
		logrus.Errorln("[batch put] proto marshal failed, err:", err)
		return err
	}

	req, err := http.NewRequest("BATCH_PUT", u, bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	tr := httpTransport
	if h.transport != nil {
		tr = h.transport(context)
	}
	res, err := tr.RoundTrip(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("server returned: %v", res.Status)
	}
	return nil
}
