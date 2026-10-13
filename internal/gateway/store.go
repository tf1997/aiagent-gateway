package gateway

import (
	"aiagent-gateway/internal/state"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type Gateway struct {
	workerRequests atomic.Int64
	remote         *sharedClient
	journal        *state.Journal
	sessionPersist sync.Mutex
	configFrozen   atomic.Bool

	callerMu     sync.Mutex
	callerCounts map[string]*atomic.Int64
	opts         Options
	current      atomic.Pointer[snapshot]
	healthy      atomic.Bool
	mu           sync.Mutex
	lifecycle    sync.RWMutex
	lock         *os.File
	legacyRaw    []byte
	shards       [256]sessionShard
	statesMu     sync.Mutex
	states       map[string]*endpointState
	schedMu      sync.Mutex
	sched        map[string]*routeSchedule
	bindings     atomic.Int64
	inflight     atomic.Int64
	requests     atomic.Uint64
	failures     atomic.Uint64
	resources    *resourceDirectory
	taskLists    a2aListCache
	sessionLists a2aListCache
	transportMu  sync.Mutex
	transports   map[int]*http.Transport
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	// Persist seam allows fault injection without changing production semantics.
	persist    func(Config) (bool, error)
	background context.Context
}

func ValidateOptions(o Options) error {
	if o.PublicURL != "" && !validURL(o.PublicURL) {
		return fmt.Errorf("invalid public URL")
	}
	if o.AdminToken == "" {
		return fmt.Errorf("admin_token required")
	}
	if strings.ContainsAny(o.AdminToken, "\r\n\x00") {
		return fmt.Errorf("admin_token must be a single-line token")
	}
	return nil
}

func Open(o Options) (*Gateway, error) {
	if err := ValidateOptions(o); err != nil {
		return nil, err
	}
	if o.MaxSessions <= 0 {
		o.MaxSessions = 100000
	}
	if o.MaxTasks <= 0 {
		o.MaxTasks = 100000
	}
	if o.MaxResources <= 0 {
		o.MaxResources = 200000
	}
	if o.MaxResourceAliases <= 0 {
		o.MaxResourceAliases = 200000
	}
	if o.MaxDataIdempotency <= 0 {
		o.MaxDataIdempotency = 100000
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
	var f *os.File
	var remote *sharedClient
	if o.StateSocket != "" {
		remote = newSharedClient(o.StateSocket, o.AdminToken, o.WorkerID)
	} else {
		if err := os.MkdirAll(o.DataDir, 0700); err != nil {
			return nil, err
		}
		var err error
		f, err = os.OpenFile(filepath.Join(o.DataDir, "gateway.lock"), os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			return nil, err
		}
		if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
			f.Close()
			return nil, fmt.Errorf("data directory already locked: %w", err)
		}
	}
	g := &Gateway{remote: remote, opts: o, lock: f, states: map[string]*endpointState{}, sched: map[string]*routeSchedule{}, resources: newResourceDirectory(o), transports: map[int]*http.Transport{}}
	g.callerCounts = map[string]*atomic.Int64{}
	g.persist = g.save
	g.resources.remote = remote
	for i := range g.shards {
		g.shards[i].items = map[sessionKey]*binding{}
	}
	fail := func(err error) (*Gateway, error) {
		if f != nil {
			syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
			f.Close()
		}
		return nil, err
	}
	var raw []byte
	var err error
	var c Config
	if remote != nil {
		var out sharedResponse
		err = remote.call("config", sharedRequest{}, &out)
		if err == nil && out.Config != nil {
			c = *out.Config
		} else {
			if err == nil {
				err = fmt.Errorf("missing shared configuration")
			}
			return fail(err)
		}
	} else {
		raw, err = os.ReadFile(filepath.Join(o.DataDir, "config.json"))
	}
	if errors.Is(err, os.ErrNotExist) && o.Initialize {
		c = Config{Schema: 2, Revision: 1, Routes: []Route{}, Receipts: map[string]Receipt{}}
		_, err = g.save(c)
	} else if err == nil && remote == nil {
		var migrated bool
		c, migrated, err = loadConfig(raw)
		if migrated {
			g.legacyRaw = raw
		}
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
	g.background = ctx
	if remote == nil && !o.DeferBackground {
		g.StartBackground()
	}
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
	g.BeginShutdown()
	g.cancel()
	g.wg.Wait()
	g.transportMu.Lock()
	for _, t := range g.transports {
		t.CloseIdleConnections()
	}
	g.transportMu.Unlock()
	if g.remote != nil {
		g.remote.client.CloseIdleConnections()
	}
	if g.lock != nil {
		syscall.Flock(int(g.lock.Fd()), syscall.LOCK_UN)
		return g.lock.Close()
	}
	return nil
}
func clone(c Config) Config {
	b, _ := json.Marshal(c)
	var out Config
	_ = json.Unmarshal(b, &out)
	return out
}

// BeginShutdown rejects new data requests before listeners drain.
func (g *Gateway) BeginShutdown() { g.lifecycle.Lock(); g.healthy.Store(false); g.lifecycle.Unlock() }

func (g *Gateway) StartBackground() {
	g.wg.Add(2)
	go g.maintenance(g.background)
	go g.reconcileLoop(g.background)
}
