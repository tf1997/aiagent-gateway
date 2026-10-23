package shared

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"aiagent-gateway/internal/gateway/protocol"
)

func responseFixture() Response {
	endpoint := protocol.Endpoint{ID: "endpoint", Interfaces: map[string]protocol.EndpointInterface{"proxy": {URL: "http://agent"}}}
	route := protocol.Route{ID: "route", Key: "test", Expose: []string{"proxy"}, Endpoints: []protocol.Endpoint{endpoint},
		Consumers: []protocol.Consumer{{ID: "alice"}}, AllowedMethods: []string{"POST"}, Card: json.RawMessage(`{"name":"agent"}`)}
	route.BuildEndpointIndex()
	return Response{Epoch: "epoch", Endpoint: &endpoint,
		Config: &protocol.Config{Routes: []protocol.Route{route},
			Receipts:     map[string]protocol.Receipt{"once": {Body: json.RawMessage(`{"ok":true}`)}},
			ReceiptOrder: []string{"once"}, Audit: []protocol.Audit{{Action: "create"}}},
		Resources: []protocol.Resource{{ID: "resource"}}, Mapped: []ResourcePair{{Value: protocol.Resource{ID: "mapped"}}},
		Cache: protocol.ListSnapshot{Tasks: []json.RawMessage{json.RawMessage(`{"id":"task"}`)}}}
}

func TestResponseCopyDetachesAllCollectionsAndIndexes(t *testing.T) {
	original := responseFixture()
	before, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	copy := copyResponse(original)
	if raw, err := json.Marshal(copy); err != nil || !bytes.Equal(raw, before) {
		t.Fatal("copy changed wire content", err)
	}
	copy.Endpoint.Interfaces["proxy"] = protocol.EndpointInterface{URL: "changed"}
	route := &copy.Config.Routes[0]
	route.Expose[0] = "changed"
	route.Endpoint("endpoint").Name = "changed through index"
	route.Endpoints[0].Interfaces["proxy"] = protocol.EndpointInterface{URL: "changed"}
	route.Consumers[0].ID = "changed"
	route.AllowedMethods[0] = "changed"
	route.Card[0] = '['
	copy.Config.Receipts["once"].Body[0] = '['
	copy.Config.ReceiptOrder[0] = "changed"
	copy.Config.Audit[0].Action = "changed"
	copy.Resources[0].ID = "changed"
	copy.Mapped[0].Value.ID = "changed"
	copy.Cache.Tasks[0][0] = '['
	if after, err := json.Marshal(original); err != nil || !bytes.Equal(before, after) {
		t.Fatal("reply or imported receipt retained a mutable alias", err)
	}
}

func TestResponseCopyRejectsInvalidRawContent(t *testing.T) {
	for _, field := range []string{"card", "receipt", "task", "empty-receipt", "empty-task", "timestamp"} {
		out := responseFixture()
		switch field {
		case "card":
			out.Config.Routes[0].Card = json.RawMessage(`{`)
		case "receipt":
			out.Config.Receipts["once"] = protocol.Receipt{Body: json.RawMessage(`{`)}
		case "task":
			out.Cache.Tasks[0] = json.RawMessage(`{`)
		case "empty-receipt":
			out.Config.Receipts["once"] = protocol.Receipt{Body: json.RawMessage{}}
		case "empty-task":
			out.Cache.Tasks[0] = json.RawMessage{}
		case "timestamp":
			out.Resource.Seen = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
		}
		if got := copyResponse(out); got.Error != "STATE_RESPONSE_INVALID" || got.Epoch != out.Epoch {
			t.Fatal("invalid response was accepted", field)
		}
	}
}

func TestResponseCopyAcceptsOmittedCardAndNullRawContent(t *testing.T) {
	out := responseFixture()
	out.Config.Routes[0].Card = json.RawMessage{}
	out.Config.Receipts["once"] = protocol.Receipt{}
	out.Cache.Tasks[0] = nil
	before, err := json.Marshal(out)
	if err != nil {
		t.Fatal(err)
	}
	if raw, err := json.Marshal(copyResponse(out)); err != nil || !bytes.Equal(raw, before) {
		t.Fatal("copy changed optional/null JSON semantics", err)
	}
}

func BenchmarkResponseCopy(b *testing.B) {
	out := responseFixture()
	b.Run("typed", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if copyResponse(out).Error != "" {
				b.Fatal("copy failed")
			}
		}
	})
	b.Run("json_round_trip", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			raw, _ := json.Marshal(out)
			var copy Response
			if err := json.Unmarshal(raw, &copy); err != nil {
				b.Fatal(err)
			}
		}
	})
}
