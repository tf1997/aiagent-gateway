// Package releaseprotocol defines product-independent local rollout contracts.
package releaseprotocol

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"
)

const Version = 1
const AuthHeader = "X-Gateway-Rollout-Auth"
const RequestHeader = "X-Gateway-Rollout-Request"
const WorkerHeader = "X-Gateway-Rollout-Worker"
const OutcomeTrailer = "X-Gateway-Rollout-Outcome"
const ProfileTrailer = "X-Gateway-Rollout-Profile"
const ReasonTrailer = "X-Gateway-Rollout-Reason"

type Identity struct {
	PID            int    `json:"pid,omitempty"`
	Protocol       int    `json:"protocol"`
	Release        string `json:"release"`
	Instance       string `json:"instance"`
	Incarnation    string `json:"incarnation"`
	ConfigRevision uint64 `json:"config_revision"`
	Ready          bool   `json:"ready"`
	Inflight       int64  `json:"inflight"`
	StateProtocol  int    `json:"state_protocol"`
}
type Target struct {
	Socket      string `json:"socket,omitempty"`
	Artifact    string `json:"artifact,omitempty"`
	Release     string `json:"release"`
	Instance    string `json:"instance"`
	Incarnation string `json:"incarnation"`
	URL         string `json:"url"`
	AdminURL    string `json:"admin_url"`
}
type Policy struct {
	Handoff              HandoffPolicy `json:"handoff"`
	AutoProfiles         bool          `json:"auto_profiles"`
	CanaryPercent        int           `json:"canary_percent"`
	ReadinessSeconds     int           `json:"readiness_timeout_seconds"`
	VerifyMinSeconds     int           `json:"verify_min_seconds"`
	VerifyTimeoutSeconds int           `json:"verify_timeout_seconds"`
	MinSuccess           int           `json:"min_success_per_profile"`
	MaxFailures          int           `json:"max_effective_failures"`
	Profiles             []string      `json:"required_profiles"`
	ObserveSeconds       int           `json:"observe_seconds"`
	ObserveMinSuccess    int           `json:"observe_min_success_per_profile"`
	DrainSeconds         int           `json:"drain_timeout_seconds"`
}

func DefaultPolicy() Policy {
	return Policy{Handoff: DefaultHandoffPolicy(), AutoProfiles: true, CanaryPercent: 5, ReadinessSeconds: 30, VerifyMinSeconds: 60, VerifyTimeoutSeconds: 300, MinSuccess: 10, MaxFailures: 0, Profiles: []string{"http"}, ObserveSeconds: 300, ObserveMinSuccess: 10, DrainSeconds: 600}
}

func (p Policy) Validate() error {
	if err := p.Handoff.Validate(); err != nil {
		return err
	}
	if p.CanaryPercent < 1 || p.CanaryPercent > 50 || p.ReadinessSeconds < 1 || p.VerifyMinSeconds < 0 || p.VerifyTimeoutSeconds < 1 || p.VerifyMinSeconds > p.VerifyTimeoutSeconds || p.MinSuccess < 1 || p.MaxFailures < 0 || p.ObserveSeconds < 1 || p.ObserveMinSuccess < 1 || p.DrainSeconds < 1 || len(p.Profiles) == 0 {
		return fmt.Errorf("invalid rollout policy")
	}
	seen := map[string]bool{}
	for _, s := range p.Profiles {
		if (s != "http" && s != "sse" && s != "a2a" && s != "a2a_sse") || seen[s] {
			return fmt.Errorf("invalid or duplicate profile %q", s)
		}
		seen[s] = true
	}
	return nil
}

type Result struct {
	ID              string    `json:"request_id"`
	Instance        string    `json:"instance"`
	Incarnation     string    `json:"incarnation"`
	Release         string    `json:"release"`
	RoutingRevision uint64    `json:"routing_revision"`
	Profile         string    `json:"profile"`
	Verdict         string    `json:"verdict"`
	Reason          string    `json:"reason"`
	Status          int       `json:"status"`
	Started         time.Time `json:"started_at"`
	Completed       time.Time `json:"completed_at"`
}
type Traffic struct {
	Revision  uint64  `json:"revision"`
	Rollout   string  `json:"rollout_id"`
	Stable    Target  `json:"stable"`
	Candidate *Target `json:"candidate,omitempty"`
	Percent   int     `json:"candidate_percent"`
	Frozen    bool    `json:"frozen"`
}
type Counts struct {
	Success  int `json:"success"`
	Failure  int `json:"failure"`
	Excluded int `json:"excluded"`
	Unknown  int `json:"unknown"`
}

func ID(prefix string) string {
	var b [16]byte
	if _, e := rand.Read(b[:]); e != nil {
		panic(e)
	}
	return prefix + hex.EncodeToString(b[:])
}

type Request struct {
	Method   string              `json:"method"`
	Path     string              `json:"path"`
	Header   map[string][]string `json:"header"`
	Body     []byte              `json:"body,omitempty"`
	BodyRead bool                `json:"body_read"`
}
type Resolution struct {
	Key         string `json:"key"`
	OwnerWorker string `json:"owner_worker,omitempty"`
	NeedBody    bool   `json:"need_body"`
	Status      int    `json:"status,omitempty"`
	Error       string `json:"error,omitempty"`
}
type Statistics struct {
	Counts   map[string]Counts `json:"counts"`
	Active   int               `json:"active"`
	Lost     bool              `json:"lost"`
	Failures int               `json:"failures"`
	Total    int               `json:"total"`
	Revision uint64            `json:"revision"`
}
