// Package runtime assembles gateway modules and coordinates scheduling,
// session bindings, lifecycle locks and local/shared state operations.
package runtime

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"aiagent-gateway/internal/gateway/a2a"
	"aiagent-gateway/internal/gateway/hermes"
	"aiagent-gateway/internal/gateway/protocol"
	configuration "aiagent-gateway/internal/gateway/runtime/config"
	resourcedir "aiagent-gateway/internal/gateway/runtime/resource"
	"aiagent-gateway/internal/gateway/runtime/rollout"
	"aiagent-gateway/internal/gateway/runtime/shared"
	"aiagent-gateway/internal/state"
)

// Options controls gateway startup, storage and runtime capacity.
type Options struct {
	// StateClient supplies an in-process dispatcher for a complete gateway.
	StateClient                                          *shared.Client
	dialContext                                          func(context.Context, string, string) (net.Conn, error)
	StateSocket, WorkerID                                string
	DataDir, AdminToken, PublicURL                       string
	DeferBackground                                      bool
	MaxSessions, MaxTasks, MaxInflight                   int
	MaxPerCaller, MaxPerEndpoint                         int
	MaxResources, MaxResourceAliases, MaxDataIdempotency int
	HealthInterval                                       time.Duration
}

type Gateway struct {
	a2aHandler     *a2a.Handler
	hermesHandler  *hermes.Handler
	worker         *rollout.Worker
	resolver       rollout.Resolver
	remote         *shared.Client
	journal        *state.Journal
	sessionPersist sync.Mutex
	configFrozen   atomic.Bool

	callerMu     sync.Mutex
	callerCounts map[string]*atomic.Int64
	opts         Options
	current      atomic.Pointer[configuration.Snapshot]
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
	resources    *resourcedir.Directory
	taskLists    *shared.ListCache
	sessionLists *shared.ListCache
	transportMu  sync.Mutex
	transports   map[int]*http.Transport
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	// Persist seam allows fault injection without changing production semantics.
	persist    func(protocol.Config) (bool, error)
	background context.Context
}

func ValidateOptions(o Options) error {
	if o.PublicURL != "" && !configuration.ValidURL(o.PublicURL) {
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
	var remote *shared.Client
	if o.StateClient != nil {
		remote = o.StateClient
	} else if o.StateSocket != "" {
		remote = shared.NewClient(o.StateSocket, o.AdminToken, o.WorkerID)
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
	g := &Gateway{
		remote: remote,
		opts:   o,
		lock:   f,
		states: map[string]*endpointState{},
		sched:  map[string]*routeSchedule{},
		resources: resourcedir.New(resourcedir.Limits{
			MaxResources:       o.MaxResources,
			MaxResourceAliases: o.MaxResourceAliases,
			MaxDataIdempotency: o.MaxDataIdempotency,
			MaxSessions:        o.MaxSessions,
			MaxTasks:           o.MaxTasks,
		}, remote),
		transports:   map[int]*http.Transport{},
		taskLists:    shared.NewListCache(nil),
		sessionLists: shared.NewListCache(nil),
		callerCounts: map[string]*atomic.Int64{},
	}
	g.persist = g.save
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
	var err error
	var c protocol.Config
	if remote != nil {
		var out shared.Response
		err = remote.Call("config", shared.Request{}, &out)
		if err == nil && out.Config != nil {
			c = *out.Config
		} else {
			if err == nil {
				err = fmt.Errorf("missing shared configuration")
			}
			return fail(err)
		}
	} else {
		c, g.legacyRaw, err = configuration.LoadStore(o.DataDir)
	}
	if err != nil {
		return fail(err)
	}
	if err = configuration.Validate(&c); err != nil {
		return fail(err)
	}
	if c.Receipts == nil {
		c.Receipts = map[string]protocol.Receipt{}
	}
	g.current.Store(configuration.Build(c))
	g.healthy.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	g.cancel = cancel
	g.background = ctx
	g.initProtocols()
	g.initRollout()
	if remote == nil && !o.DeferBackground {
		g.StartBackground()
	}
	return g, nil
}

func (g *Gateway) save(c protocol.Config) (bool, error) { return configuration.Save(g.opts.DataDir, c) }

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
		g.remote.Close()
	}
	if g.lock != nil {
		syscall.Flock(int(g.lock.Fd()), syscall.LOCK_UN)
		return g.lock.Close()
	}
	return nil
}

// BeginShutdown rejects new data requests before listeners drain.
func (g *Gateway) BeginShutdown() { g.lifecycle.Lock(); g.healthy.Store(false); g.lifecycle.Unlock() }

// StateHealthy includes configuration persistence failures as well as the
// domain journal. Public readiness must not advertise an unusable state owner.
func (g *Gateway) StateHealthy() bool {
	return g.healthy.Load() && (g.journal == nil || g.journal.Healthy() == nil)
}

func (g *Gateway) StartBackground() {
	g.wg.Add(2)
	go g.maintenance(g.background)
	go g.reconcileLoop(g.background)
}
