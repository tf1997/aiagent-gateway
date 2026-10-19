package service

import (
	p "aiagent-gateway/internal/releaseprotocol"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

func workerIdentity(ctx context.Context, token string, t p.Target) (p.Identity, error) {
	var id p.Identity
	req, err := http.NewRequestWithContext(ctx, "GET", t.AdminURL+"/control/v1/identity", nil)
	if err != nil {
		return id, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	client := http.Client{Timeout: 2 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	res, err := client.Do(req)
	if err != nil {
		return id, err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return id, fmt.Errorf("worker control HTTP %d", res.StatusCode)
	}
	err = json.NewDecoder(io.LimitReader(res.Body, 64<<10)).Decode(&id)
	return id, err
}
