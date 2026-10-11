package gateway

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
)

const maxRouteConsumers = 1000

func consumerTokenHash(token string) string {
	digest := sha256.Sum256([]byte(token))
	return hex.EncodeToString(digest[:])
}

func validateConsumers(consumers []Consumer) error {
	if len(consumers) > maxRouteConsumers {
		return fmt.Errorf("TOO_MANY_CONSUMERS")
	}
	ids, hashes := map[string]bool{}, map[string]bool{}
	for _, c := range consumers {
		digest, err := hex.DecodeString(c.TokenHash)
		if !validKey.MatchString(c.ID) || err != nil || len(digest) != sha256.Size || c.TokenHash != strings.ToLower(c.TokenHash) {
			return fmt.Errorf("INVALID_CONSUMER")
		}
		if ids[c.ID] || hashes[c.TokenHash] {
			return fmt.Errorf("DUPLICATE_CONSUMER_ID_OR_TOKEN")
		}
		ids[c.ID], hashes[c.TokenHash] = true, true
	}
	return nil
}

// Normalize separately from Route decoding so callers cannot supply token_hash
// or cause tokens to be included in decoder errors and API responses.
func (g *Gateway) consumerInput(fields map[string]json.RawMessage, previous []Consumer) ([]Consumer, bool, error) {
	raw, provided := fields["consumers"]
	if !provided {
		return previous, false, nil
	}
	delete(fields, "consumers")
	var inputs []struct {
		ID    string          `json:"id"`
		Token json.RawMessage `json:"token"`
	}
	if err := decode(raw, &inputs); err != nil || inputs == nil {
		return nil, true, fmt.Errorf("INVALID_CONSUMERS: expected an array of id/token objects")
	}
	if len(inputs) > maxRouteConsumers {
		return nil, true, fmt.Errorf("TOO_MANY_CONSUMERS")
	}
	old := map[string]string{}
	for _, c := range previous {
		old[c.ID] = c.TokenHash
	}
	consumers := make([]Consumer, 0, len(inputs))
	for _, input := range inputs {
		hash := old[input.ID]
		if len(input.Token) != 0 {
			var token string
			if json.Unmarshal(input.Token, &token) != nil || token == "" || len(token) > 4096 || strings.ContainsAny(token, "\r\n\x00") || token == g.opts.AdminToken {
				return nil, true, fmt.Errorf("INVALID_CONSUMER_TOKEN")
			}
			hash = consumerTokenHash(token)
		}
		if hash == "" {
			return nil, true, fmt.Errorf("CONSUMER_TOKEN_REQUIRED")
		}
		consumers = append(consumers, Consumer{ID: input.ID, TokenHash: hash})
	}
	if err := validateConsumers(consumers); err != nil {
		return nil, true, err
	}
	return consumers, true, nil
}

func routeConsumer(rt *Route, owner string) bool {
	for _, c := range rt.Consumers {
		if c.ID == owner {
			return true
		}
	}
	return false
}

func authenticateConsumer(rt *Route, authorization string) *Caller {
	if !strings.HasPrefix(authorization, "Bearer ") {
		return nil
	}
	token := strings.TrimPrefix(authorization, "Bearer ")
	if token == "" || len(token) > 4096 {
		return nil
	}
	hash := consumerTokenHash(token)
	for _, c := range rt.Consumers {
		if subtle.ConstantTimeCompare([]byte(hash), []byte(c.TokenHash)) == 1 {
			return &Caller{ID: c.ID}
		}
	}
	return nil
}

type consumerView struct {
	ID string `json:"id"`
}
type routeView struct {
	*Route
	Consumers []consumerView `json:"consumers"`
}

func publicRoute(rt *Route) routeView {
	v := routeView{Route: rt, Consumers: make([]consumerView, 0, len(rt.Consumers))}
	for _, c := range rt.Consumers {
		v.Consumers = append(v.Consumers, consumerView{ID: c.ID})
	}
	return v
}

// Counter entries exist only while requests are active. Token rotation keeps
// the same owner; removing a consumer cannot reset an in-flight concurrency limit.
func (g *Gateway) beginCaller(owner string) (*atomic.Int64, int64) {
	g.callerMu.Lock()
	defer g.callerMu.Unlock()
	c := g.callerCounts[owner]
	if c == nil {
		c = &atomic.Int64{}
		g.callerCounts[owner] = c
	}
	return c, c.Add(1)
}
func (g *Gateway) endCaller(owner string, c *atomic.Int64) {
	g.callerMu.Lock()
	defer g.callerMu.Unlock()
	if c.Add(-1) == 0 {
		delete(g.callerCounts, owner)
	}
}
