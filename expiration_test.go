package groupcache

import (
	"testing"
	"time"
	"fmt"
	"errors"
	pb "github.com/cyber4ron/groupcache/groupcachepb"
)

var (
	timestampGroup *Group

	// cFills is the number of times timestampGroup Getter has been called.
	// Read using the cFills function.
	cFills AtomicInt
)

type testTimeProvider struct {
	NowChannel   chan int64
	AfterChannel chan time.Time
}

func (t testTimeProvider) NowUnixSeconds() int64 {
	if len(t.NowChannel) == 0 {
		panic("empty nowChannel queue") // Keep miswritten tests from hanging.
	}
	newTime := <-t.NowChannel
	return newTime
}

func (t testTimeProvider) After(d time.Duration) <-chan time.Time {
	return t.AfterChannel
}

var nowChannel chan int64
var afterChannel chan time.Time
var contentChannel chan string

func testingSetup(groupName string) {
	nowChannel = make(chan int64, 10)
	afterChannel = make(chan time.Time, 10)
	contentChannel = make(chan string, 10)
	peerChannel = make(chan int, 10)
	tp := testTimeProvider{NowChannel: nowChannel, AfterChannel: afterChannel}
	setTimeProvider(tp)

	timestampGroup = NewGroup(groupName, 1 << 20, GetterFunc(func(_ Context, key string, dest Sink) error {
		if len(contentChannel) == 0 {
			panic("empty contentChannel queue")
		}
		content := []byte(<-contentChannel)
		cFills.Add(1)
		// 1 ms delay Should be sufficient to make functionality of select logic in
		// handleExpiration() deterministic.  The test passes without this, but we
		// should be conservative.
		time.Sleep(1 * time.Millisecond)
		return dest.SetTimestampBytes(content, GetUnixTime())
	}), nil)
}

func callGet(t *testing.T, key string) (content []byte, timestamp int64) {
	var packedContent []byte
	if err := timestampGroup.Get(nil, key, AllocatingByteSliceSink(&packedContent)); err != nil {
		t.Fatal(err)
	}
	content, timestamp, err := UnpackTimestamp(packedContent)
	if err != nil {
		t.Fatal(err)
	}
	return content, timestamp
}

func verifyContent(t *testing.T, want string, got []byte) {
	if string(got) != want {
		t.Errorf("Got: %s, Want:%s", string(got), want)
	}
}

func verifyTimestamp(t *testing.T, want int64, got int64) {
	if got != want {
		t.Errorf("Got: %d, Want:%d", got, want)
	}
}

func verifyInt64(t *testing.T, want int64, got int64) {
	if got != want {
		t.Errorf("Got: %d, Want:%d", got, want)
	}
}

func TestExpires(t *testing.T) {
	testingSetup("expires-group")
	testKey := "testKey1"
	timestampGroup.SetExpiration(time.Duration(300) * time.Second)
	timestampGroup.SetLoadTimeout(time.Duration(10) * time.Second)

	startFills := cFills.Get()

	contentChannel <- "blahblah1"
	nowChannel <- 100 				// Becomes data timestamp
	content, timestamp := callGet(t, testKey)
	verifyContent(t, "blahblah1", content)
	verifyTimestamp(t, 100, timestamp)

	nowChannel <- 399 			 	// Used for expiration check.
	content, timestamp = callGet(t, testKey) 	// cache hit
	verifyContent(t, "blahblah1", content)
	verifyTimestamp(t, 100, timestamp)

	// afterChannel <- time.Time{}
	nowChannel <- 400                        	// Used for expiration check. will trig expiration
	nowChannel <- 401                        	// Used for load
	nowChannel <- 401                        	// Used for data timestamp
	contentChannel <- "foofoo1"              	// New content.
	content, timestamp = callGet(t, testKey)
	verifyContent(t, "foofoo1", content)
	verifyTimestamp(t, 401, timestamp)

	nowChannel <- 700 				// will not trig expiration
	content, timestamp = callGet(t, testKey)
	verifyContent(t, "foofoo1", content)
	verifyTimestamp(t, 401, timestamp)

	// afterChannel <- time.Time{}
	nowChannel <- 701                        	// Used for expiration check. will trig expiration
	nowChannel <- 702                        	// Used for load
	nowChannel <- 702                        	// Used for data timestamp.
	contentChannel <- "barbar1"              	// New content.
	content, timestamp = callGet(t, testKey)
	verifyContent(t, "barbar1", content)
	verifyTimestamp(t, 702, timestamp)

	printGroupStat()

	totalFills := cFills.Get() - startFills
	if totalFills != 3 {
		t.Errorf("Wanted 3, got %d", totalFills)
	}
}

var peerChannel chan int

type MockPeerPicker struct {
	noWatchImplementation
	PeerList []MockGetter
}

func NewMockPeerPicker() *MockPeerPicker {
	pp := &MockPeerPicker{PeerList: []MockGetter{{true}, {true}}}
	RegisterPeerPicker(func() PeerPicker { return pp })
	return pp
}

func (pp *MockPeerPicker) PickPeer(key string) (ProtoGetter, bool) {
	if len(peerChannel) == 0 {
		panic("empty peerChannel queue")
	}
	return &pp.PeerList[<-peerChannel], true
}

type MockGetter struct {
	IsServing bool
}

func (mg *MockGetter) Get(context Context, in *pb.GetRequest, out *pb.GetResponse) error {
	if mg.IsServing {
		if len(contentChannel) == 0 {
			panic("empty contentChannel queue")
		}
		if len(nowChannel) == 0 {
			panic("empty nowChannel queue")
		}
		timeTs := <-nowChannel
		out.Value, _ = packTimestamp([]byte(<-contentChannel), timeTs)
		return nil
	} else {
		return errors.New("peer down.")
	}
}

func printGroupStat() {
	fmt.Printf("stat: %+v\n", timestampGroup.Stats)
	fmt.Printf("hot cache: %+v\n", timestampGroup.hotCache.stats())
	fmt.Printf("main cache: %+v\n", timestampGroup.mainCache.stats())
	fmt.Println("====")
}

func TestExpires2(t *testing.T) {
	testingSetup("expires-group")
	timestampGroup.SetExpiration(time.Duration(300) * time.Second)

	peers := NewMockPeerPicker()
	peerNo := 1
	peers.PeerList[peerNo].IsServing = false 		// peer down

	testKey := "test"
	nowChannel <- 100
	contentChannel <- "blahblah1"
	peerChannel <- peerNo
	content, timestamp := callGet(t, testKey) 		// will cache in main cache
	verifyContent(t, "blahblah1", content)
	verifyTimestamp(t, 100, timestamp)
	verifyInt64(t, 21, timestampGroup.mainCache.stats().Bytes)

	printGroupStat()

	nowChannel <- 401                        		// Used for expiration check. will trig expiration
	nowChannel <- 402                        		// Used for load
	nowChannel <- 403                        		// Used for data timestamp.
	contentChannel <- "foofoo1"
	peerChannel <- peerNo
	peers.PeerList[peerNo].IsServing = true 		// peer up
	content, timestamp = callGet(t, testKey)		// probably cache in peer and remove from hot cache
	verifyContent(t, "foofoo1", content)
	verifyTimestamp(t, 403, timestamp)
	verifyContent(t, "foofoo1", content)
	value, _ := packTimestamp([]byte("foofoo1"), 403)	// make sure cache in hot cache
	timestampGroup.hotCache.add(testKey, ByteView{b: value})
	verifyInt64(t, 19, timestampGroup.hotCache.stats().Bytes)
	verifyInt64(t, 1, timestampGroup.mainCache.stats().Evictions)

	printGroupStat()

	nowChannel <- 703                        		// Used for expiration check. will trig expiration
	nowChannel <- 704                        		// Used for load
	nowChannel <- 705                        		// Used for data timestamp.
	contentChannel <- "barbar1"              		// New content.
	peerChannel <- peerNo
	peers.PeerList[peerNo].IsServing = false		// peer down
	content, timestamp = callGet(t, testKey)		// will cache in main cache and remove from hot cache
	verifyTimestamp(t, 705, timestamp)
	verifyContent(t, "barbar1", content)
	verifyInt64(t, 1, timestampGroup.hotCache.stats().Evictions) // maybe error, because object are added to hot cache by chance
	verifyInt64(t, 19, timestampGroup.mainCache.stats().Bytes)

	printGroupStat()
}