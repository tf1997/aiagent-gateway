package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type Gateway struct {
	callerCounts   map[string]*atomic.Int64
	taskReverse    map[upstreamKey]string
	contextReverse map[upstreamKey]string
	opts           Options
	current        atomic.Pointer[snapshot]
	healthy        atomic.Bool
	mu             sync.Mutex
	lifecycle      sync.RWMutex
	lock           *os.File
	shards         [256]sessionShard
	statesMu       sync.Mutex
	states         map[string]*endpointState
	schedMu        sync.Mutex
	sched          map[string]*routeSchedule
	bindings       atomic.Int64
	inflight       atomic.Int64
	requests       atomic.Uint64
	failures       atomic.Uint64
	a2aMu          sync.Mutex
	tasks          map[string]*taskBinding
	contexts       map[string]*contextBinding
	transportMu    sync.Mutex
	transports     map[int]*http.Transport
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	// Persist seam allows fault injection without changing production semantics.
	persist func(Config) (bool, error)
}

func Open(o Options) (*Gateway, error) {
	if o.PublicURL != "" && !validURL(o.PublicURL) {
		return nil, fmt.Errorf("invalid public URL")
	}
	if o.AdminToken == "" || len(o.Callers) == 0 {
		return nil, fmt.Errorf("admin token and callers required")
	}
	seen := map[string]bool{}
	for _, c := range o.Callers {
		if c.ID == "" || c.Token == "" || seen[c.ID] || seen[c.Token] || c.Token == o.AdminToken {
			return nil, fmt.Errorf("unique caller IDs and distinct tokens required")
		}
		seen[c.ID] = true
		seen[c.Token] = true
	}
	if o.MaxSessions <= 0 {
		o.MaxSessions = 100000
	}
	if o.MaxTasks <= 0 {
		o.MaxTasks = 100000
	}
	if o.MaxInflight <= 0 {
		o.MaxInflight = 4096
	}
	if o.MaxPerCaller <= 0 {
		o.MaxPerCaller = 1024
	}
	if o.MaxPerEndpoint <= 0 {
		o.MaxPerEndpoint = 256
	}
	if o.HealthInterval <= 0 {
		o.HealthInterval = 10 * time.Second
	}
	if err := os.MkdirAll(o.DataDir, 0700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(o.DataDir, "gateway.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("data directory already locked: %w", err)
	}
	g := &Gateway{opts: o, lock: f, states: map[string]*endpointState{}, sched: map[string]*routeSchedule{}, tasks: map[string]*taskBinding{}, contexts: map[string]*contextBinding{}, transports: map[int]*http.Transport{}}
	g.taskReverse = map[upstreamKey]string{}
	g.contextReverse = map[upstreamKey]string{}
	g.callerCounts = map[string]*atomic.Int64{}
	for _, c := range o.Callers {
		g.callerCounts[c.ID] = &atomic.Int64{}
	}
	g.persist = g.save
	for i := range g.shards {
		g.shards[i].items = map[sessionKey]*binding{}
	}
	fail := func(err error) (*Gateway, error) {
		syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		f.Close()
		return nil, err
	}
	raw, err := os.ReadFile(filepath.Join(o.DataDir, "config.json"))
	var c Config
	if errors.Is(err, os.ErrNotExist) && o.Initialize {
		c = Config{Schema: 1, Revision: 1, Routes: []Route{}, Receipts: map[string]Receipt{}}
		_, err = g.save(c)
	} else if err == nil {
		err = decode(raw, &c)
	}
	if err != nil {
		return fail(err)
	}
	if err = validate(&c); err != nil {
		return fail(err)
	}
	if c.Receipts == nil {
		c.Receipts = map[string]Receipt{}
	}
	g.current.Store(build(c))
	g.healthy.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	g.cancel = cancel
	g.wg.Add(1)
	go g.maintenance(ctx)
	return g, nil
}
func (g *Gateway) save(c Config) (renamed bool, err error) {
	b, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return false, err
	}
	f, err := os.CreateTemp(g.opts.DataDir, ".config-*.tmp")
	if err != nil {
		return false, err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(b); err != nil {
		f.Close()
		return false, err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return false, err
	}
	if err = f.Close(); err != nil {
		return false, err
	}
	if err = os.Rename(f.Name(), filepath.Join(g.opts.DataDir, "config.json")); err != nil {
		return false, err
	}
	d, err := os.Open(g.opts.DataDir)
	if err != nil {
		return true, err
	}
	defer d.Close()
	return true, d.Sync()
}
func (g *Gateway) Close() error {
	g.healthy.Store(false)
	g.cancel()
	g.wg.Wait()
	g.transportMu.Lock()
	for _, t := range g.transports {
		t.CloseIdleConnections()
	}
	g.transportMu.Unlock()
	syscall.Flock(int(g.lock.Fd()), syscall.LOCK_UN)
	return g.lock.Close()
}
func clone(c Config) Config {
	b, _ := json.Marshal(c)
	var out Config
	_ = json.Unmarshal(b, &out)
	return out
}

// BeginShutdown rejects new data requests before listeners drain.
func (g *Gateway) BeginShutdown() { g.healthy.Store(false) }
