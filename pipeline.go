package groupcache

import (
	"strconv"
	"time"
	"errors"
	"github.com/Sirupsen/logrus"
)

type Response struct {
	Idx int
	Err error
}

type Request struct {
	Ctx  Context
	Idx  int
	Key  string
	Sink Sink
	Resp chan *Response
}

var (
	ErrMGetTimeout = errors.New("mget timeout")
)

type Pipeline struct {
	Concurrency int
	Capacity    int
	Process     ProcessFunc
	shutdown    chan struct{}
	pipeline    chan *Request

	MGetTimeout time.Duration
	Group       *Group
}

type ProcessFunc func(g *Group, req *Request) *Response
var defaultProcessFunc = func(g *Group, req *Request) *Response {
	err := g.Get(req.Ctx, req.Key, req.Sink)
	return &Response{req.Idx, err}
}

func (p *Pipeline) bgCheck() {
	go func() {
		for {
			logrus.Infof("pipeline length: %d", len(p.pipeline))
			time.Sleep(time.Second)
		}
	}()
}

func (p *Pipeline) Start() {
	if p.shutdown == nil {
		p.shutdown = make(chan struct{})
	}
	if p.pipeline == nil {
		p.pipeline = make(chan *Request, p.Capacity)
	}
	for i := 0; i < p.Concurrency; i++ {
		go p.process(i, p.shutdown)
	}
	p.bgCheck()
}

func (p *Pipeline) Put(req *Request) {
	p.pipeline <- req
}

// TODO corner cases?
func (p *Pipeline) Shutdown() {
	close(p.shutdown)
}

func (g *Group) ShutdownPipeline() {
	g.pipeline.Shutdown()
}

func (p *Pipeline) process(i int, shutdown <-chan struct{}) {
	logrus.Infoln("processor %d start", strconv.Itoa(i))
	go func() {
		for {
			select {
			case req := <-p.pipeline:
				req.Resp <- p.Process(p.Group, req)
			case <-shutdown:
				logrus.Infoln("processor %d stopped", strconv.Itoa(i))
				return
			}
		}
	}()
}

func (g *Group) MGet(ctx Context, keys []string, sinks []Sink) (error, []error) {
	if len(keys) != len(sinks) {
		return errors.New("length of keys not equals length of sinks"), nil
	}

	length := len(keys)
	errs := make([]error, length)
	fills := make([]bool, length)
	timeout := time.After(g.opt.PipelineMGetTimeout)
	ch := make(chan *Response, length)
	for i := 0; i < length; i++ {
		g.pipeline.Put(&Request{ctx, i, keys[i], sinks[i], ch})
	}

	count := 0
	loop: for {
		if count == length {
			break
		}
		select {
		case resp := <-ch:
			count++
			errs[resp.Idx] = resp.Err
			fills[resp.Idx] = true
		case <-timeout:
			for i, b := range fills {
				if !b {
					errs[i] = ErrMGetTimeout
				}
			}
			break loop
		}
	}

	return nil, errs
}