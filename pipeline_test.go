package groupcache

import (
	"testing"
	"fmt"
	"strconv"
	"time"
)

var (
	group *Group
)

func pipelineTestSetup() {
	group = newGroupOpts("test", 1 << 20, GetterFunc(func(ctx Context, key string, dest Sink) error {
		return dest.SetString(key)
	}), nil, nil, &GroupOpts{
		PipelineConcurrency: 5,
		PipelineCapacity: 2,
		PipelineMGetTimeout: time.Second * 10,
	})
}

func assertEqualsStringArray(t *testing.T, msg string, want []string, got []string) {
	if len(want) != len(got) {
		t.Errorf("%s, Got: %v, Want:%v", msg, got, want)
		return
	}
	for i, e := range want {
		if got[i] != e {
			t.Errorf("%s, Got: %v, Want:%v", msg, got, want)
			return
		}
	}
}

func TestPipeline(t *testing.T) {
	mgetKeysNum := 10
	pipelineTestSetup()
	defer group.ShutdownPipeline()

	keys := make([]string, mgetKeysNum)
	bytes := make([][]byte, mgetKeysNum)
	sinks := make([]Sink, mgetKeysNum)

	for i := 0; i < mgetKeysNum; i++ {
		keys[i] = strconv.Itoa(i)
		sinks[i] = AllocatingByteSliceSink(&bytes[i])
	}
	err, errs := group.MGet(nil, keys, sinks)

	if err != nil {
		fmt.Println(err)
	}

	want := make([]string, mgetKeysNum)
	for i := 0; i < mgetKeysNum; i++ {
		want[i] = strconv.Itoa(i)
	}

	got := make([]string, mgetKeysNum)
	for i, err := range errs {
		if err != nil {
			fmt.Errorf("%d, %v\n", i, err)
		} else {
			got[i] = string(bytes[i][:])
			// fmt.Printf("key: %s, val: %s\n", keys[i], string(bytes[i][:]))
		}
	}

	assertEqualsStringArray(t, "mget test", want, got)
}
