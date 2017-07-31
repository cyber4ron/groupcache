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
	"errors"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"hash/crc32"
	"sort"
	"fmt"

	"github.com/samuel/go-zookeeper/zk"
	pb "github.com/cyber4ron/groupcache/groupcachepb"
)

var (
	peerAddrs = flag.String("test_peer_addrs", "", "Comma-separated list of peer addresses; used by TestHTTPPool")
	peerIndex = flag.Int("test_peer_index", -1, "Index of which peer this child is; used by TestHTTPPool")
	peerChild = flag.Bool("test_peer_child", false, "True if running as a child process; used by TestHTTPPool")
)

func TestHTTPPool(t *testing.T) {
	if *peerChild {
		beChildForTestHTTPPool()
		os.Exit(0)
	}

	const (
		nChild = 4
		nGets  = 100
	)

	var childAddr []string
	for i := 0; i < nChild; i++ {
		childAddr = append(childAddr, pickFreeAddr(t))
	}

	var cmds []*exec.Cmd
	var wg sync.WaitGroup
	for i := 0; i < nChild; i++ {
		cmd := exec.Command(os.Args[0],
			"--test.run=TestHTTPPool",
			"--test_peer_child",
			"--test_peer_addrs="+strings.Join(childAddr, ","),
			"--test_peer_index="+strconv.Itoa(i),
		)
		cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
		cmds = append(cmds, cmd)
		wg.Add(1)
		if err := cmd.Start(); err != nil {
			t.Fatal("failed to start child process: ", err)
		}
		go awaitAddrReady(t, childAddr[i], &wg)
	}
	defer func() {
		for i := 0; i < nChild; i++ {
			if cmds[i].Process != nil {
				cmds[i].Process.Kill()
			}
		}
	}()
	wg.Wait()

	// Use a dummy self address so that we don't handle gets in-process.
	p := NewHTTPPoolOpts("should-be-ignored", &HTTPPoolOptions{
		BasePath: "/_groupcache/",
		ConfigOpts: ConfigOpts{"127.0.0.1:2181", "/common", time.Second},
	})
	p.Set(addrToURL(childAddr)...)

	// Dummy getter function. Gets should go to children only.
	// The only time this process will handle a get is when the
	// children can't be contacted for some reason.
	getter := GCGetter([]interface{}{func(ctx Context, key string, dest Sink) error {
		return errors.New("parent getter called; something's wrong")
	}})
	g := NewGroup("httpPoolTest", 1<<20, getter, nil)

	for _, key := range testKeys(nGets) {
		var value string
		if err := g.Get(nil, key, StringSink(&value)); err != nil {
			t.Fatal(err)
		}
		if suffix := ":" + key; !strings.HasSuffix(value, suffix) {
			t.Errorf("Get(%q) = %q, want value ending in %q", key, value, suffix)
		}
		t.Logf("Get key=%q, value=%q (peer:key)", key, value)
	}
}

func testKeys(n int) (keys []string) {
	keys = make([]string, n)
	for i := range keys {
		keys[i] = strconv.Itoa(i)
	}
	return
}

func beChildForTestHTTPPool() {
	addrs := strings.Split(*peerAddrs, ",")

	p := NewHTTPPoolOpts("http://" + addrs[*peerIndex], &HTTPPoolOptions{
		BasePath: "/_groupcache/",
		ConfigOpts: ConfigOpts{"127.0.0.1:2181", "/common", time.Second},
	})
	p.Set(addrToURL(addrs)...)

	getter := GCGetter([]interface{}{func(ctx Context, key string, dest Sink) error {
		dest.SetString(strconv.Itoa(*peerIndex) + ":" + key)
		return nil
	}})
	NewGroup("httpPoolTest", 1<<20, getter, nil)

	http.Handle(p.opts.BasePath, p)
	log.Fatal(http.ListenAndServe(addrs[*peerIndex], nil))
}

// This is racy. Another process could swoop in and steal the port between the
// call to this function and the next listen call. Should be okay though.
// The proper way would be to pass the l.File() as ExtraFiles to the child
// process, and then close your copy once the child starts.
func pickFreeAddr(t *testing.T) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func addrToURL(addr []string) []string {
	url := make([]string, len(addr))
	for i := range addr {
		url[i] = "http://" + addr[i]
	}
	return url
}

func awaitAddrReady(t *testing.T, addr string, wg *sync.WaitGroup) {
	defer wg.Done()
	const max = 1 * time.Second
	tries := 0
	for {
		tries++
		c, err := net.Dial("tcp", addr)
		if err == nil {
			c.Close()
			return
		}
		delay := time.Duration(tries) * 25 * time.Millisecond
		if delay > max {
			delay = max
		}
		time.Sleep(delay)
	}
}

func AddZNode(conn *zk.Conn, path string) {
	_, err := conn.Create(path, nil, int32(zk.FlagEphemeral), zk.WorldACL(zk.PermAll))
	if err != nil {
		fmt.Println(err)
	}
}

func DeleteZNode(conn *zk.Conn, path string) {
	err := conn.Delete(path, -1)
	if err != nil {
		fmt.Println(err)
	}
}

func verifyArrayInt(t *testing.T, msg string, want []int, got []int) {
	if len(want) != len(got) {
		t.Errorf("%s, Got: %d, Want:%d", msg, got, want)
		return
	}
	for i, e := range want {
		if got[i] != e {
			t.Errorf("%s, Got: %d, Want:%d", msg, got, want)
			return
		}
	}
}

func verifyString(t *testing.T, want, got string) {
	if want != got {
		t.Errorf("Got: %s, Want:%s", got, want)
	}
}

func getKeys(replicas int, urls ...string) []int {
	var keys []int
	hashMap := make(map[int]string)
	for _, url := range urls {
		for i := 0; i < replicas; i++ {
			hash := int(crc32.ChecksumIEEE([]byte(strconv.Itoa(i) + url)))
			keys = append(keys, hash)
			hashMap[hash] = url
		}
	}
	sort.Ints(keys)
	return keys
}

func TestConfigWatch(t *testing.T) {
	servers := "127.0.0.1:2181"
	basePath := ""
	url0 := "http://172.10.10.0:9099"
	url1 := "http://172.10.10.1:9099"
	url2 := "http://172.10.10.2:9099"
	url3 := "http://172.10.10.3:9099"
	sessionTimeout := time.Second
	replicas := 2

	time.Sleep(sessionTimeout)

	conn, _, err := zk.Connect(strings.Split(servers, ","), sessionTimeout)
	if err != nil {
		panic(err)
	}

	time.Sleep(sessionTimeout)

	// start http pool1
	p := NewHTTPPoolOpts(url0, &HTTPPoolOptions{
		Replicas : replicas,
		ConfigOpts: ConfigOpts{servers, basePath, sessionTimeout},
	})
	err = p.RegisterAndWatch()
	if err != nil {
		panic(err)
	}

	time.Sleep(sessionTimeout)

	verifyArrayInt(t, "url0", getKeys(replicas, url0), p.peers.Keys())

	// start http pool2
	httpPoolMade = false
	portPicker = nil
	p1 := NewHTTPPoolOpts(url1, &HTTPPoolOptions{
		Replicas : replicas,
		ConfigOpts: ConfigOpts{servers, basePath, sessionTimeout},
	})
	err = p1.RegisterAndWatch()
	if err != nil {
		panic(err)
	}

	// add znode
	AddZNode(conn, strings.Join([]string{basePath, GroupCacheNodesZNode, url2[7:]}, "/"))

	time.Sleep(sessionTimeout)
	verifyArrayInt(t, "url0, url1, url2", getKeys(replicas, url0, url1, url2), p.peers.Keys())
	verifyArrayInt(t, "url0, url1, url2", getKeys(replicas, url0, url1, url2), p1.peers.Keys())

	// add znode
	AddZNode(conn, strings.Join([]string{basePath, GroupCacheNodesZNode, url3[7:]}, "/"))

	time.Sleep(sessionTimeout)
	verifyArrayInt(t, "url0, url1, url2, url3", getKeys(replicas, url0, url1, url2, url3), p.peers.Keys())
	verifyArrayInt(t, "url0, url1, url2, url3", getKeys(replicas, url0, url1, url2, url3), p1.peers.Keys())
	// delete znode
	DeleteZNode(conn, strings.Join([]string{basePath, GroupCacheNodesZNode, url3[7:]}, "/"))

	time.Sleep(sessionTimeout)
	verifyArrayInt(t, "url0, url1, url2", getKeys(replicas, url0, url1, url2), p.peers.Keys())
	verifyArrayInt(t, "url0, url1, url2", getKeys(replicas, url0, url1, url2), p1.peers.Keys())

	// close p1.configWatcher
	p1.configWatcher.Close()

	time.Sleep(sessionTimeout)
	verifyArrayInt(t, "url0, url2", getKeys(replicas, url0, url2), p.peers.Keys())

	time.Sleep(time.Second * 10)
}

func TestHttpGetter_Put(t *testing.T) {
	// create gc group
	getter := GCGetter([]interface{}{func(ctx Context, key string, dest Sink) error {
		dest.SetString(strconv.Itoa(*peerIndex) + ":" + key)
		return nil
	}})

	g := NewGroup("httpPoolTest", 1<<10, getter, nil)

	// start http pool
	pool := NewHTTPPoolOpts("http://127.0.0.1:9090", &HTTPPoolOptions{
		BasePath: "/_groupcache/",
		ConfigOpts: ConfigOpts{"127.0.0.1:2181", "/common", time.Second},})
	if err := pool.RegisterAndWatch(); err != nil {
		panic(err)
	}
	http.Handle("/_groupcache/", pool)
	go http.ListenAndServe("127.0.0.1:9090", nil)
	time.Sleep(time.Second * 1)

	// pack req
	key := "001"
	req := &pb.PutRequest{
		Group: &g.name,
		Key:   &key,
		Value: []byte("x"),
	}

	// test Put
	httpGetter := httpGetter{transport: func(Context) http.RoundTripper {
		return httpTransport
	}, baseURL: "http://127.0.0.1:9090/_groupcache/"}

	res := &pb.PutResponse{}
	err := httpGetter.Put(nil, req, res)
	if err != nil {
		fmt.Println("put err:", err)
	}

	var value string
	if err := g.Get(nil, "001", StringSink(&value)); err != nil {
		t.Fatal(err)
	}
	r, _, _ := UnpackTimestamp([]byte(value))

	fmt.Println("r:", string(r))
	verifyString(t, string(r), "x")
}


func TestHttpGetter_PutBatch(t *testing.T) {
	// create gc group
	getter := GCGetter([]interface{}{func(ctx Context, key string, dest Sink) error {
		dest.SetString(strconv.Itoa(*peerIndex) + ":" + key)
		return nil
	}})

	g := NewGroup("httpPoolTest", 1<<10, getter, nil)

	// start http pool
	pool := NewHTTPPoolOpts("http://127.0.0.1:9090", &HTTPPoolOptions{
		BasePath: "/_groupcache/",
		ConfigOpts: ConfigOpts{"127.0.0.1:2181", "/common", time.Second},})
	if err := pool.RegisterAndWatch(); err != nil {
		panic(err)
	}
	http.Handle("/_groupcache/", pool)
	go http.ListenAndServe("127.0.0.1:9090", nil)
	time.Sleep(time.Second * 1)

	// pack req
	keys := []string{"001", "002", "003"}
	req := &pb.PutBatchRequest{
		Group: &g.name,
		Keys:   keys,
		Values: [][]byte{[]byte("x"), []byte("y"), []byte("z")},
	}

	// test PutBatch
	httpGetter := httpGetter{transport: func(Context) http.RoundTripper {
		return httpTransport
	}, baseURL: "http://127.0.0.1:9090/_groupcache/"}

	res := &pb.PutBatchResponse{}
	err := httpGetter.PutBatch(nil, req, res)
	if err != nil {
		fmt.Println("put err:", err)
	}

	var value string
	if err := g.Get(nil, "001", StringSink(&value)); err != nil {
		t.Fatal(err)
	}
	r, _, _ := UnpackTimestamp([]byte(value))

	fmt.Println("r:", string(r))
	verifyString(t, string(r), "x")


	if err := g.Get(nil, "002", StringSink(&value)); err != nil {
		t.Fatal(err)
	}
	r, _, _ = UnpackTimestamp([]byte(value))
	fmt.Println("r:", string(r))
	verifyString(t, string(r), "y")
}