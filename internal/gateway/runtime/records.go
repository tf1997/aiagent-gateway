package runtime

import (
	"cmp"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"aiagent-gateway/internal/gateway/protocol"
	resourcedir "aiagent-gateway/internal/gateway/runtime/resource"
	"aiagent-gateway/internal/gateway/runtime/shared"
	"aiagent-gateway/internal/state"
)

// StateRecords indexes a baseline by stable identities. Subsequent changes
// replace only these records; array offsets never become replication keys.
func StateRecords(raw []byte) (map[string]json.RawMessage, error) {
	var v stateImage
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, err
	}
	if v.Format != stateImageFormat {
		return nil, fmt.Errorf("UNSUPPORTED_STATE_IMAGE")
	}
	records := map[string]json.RawMessage{}
	put := func(key string, value any) { records[key], _ = json.Marshal(value) }
	put("runtime/config", v.Config)
	put("runtime/legacy", v.LegacyConfig)
	put("runtime/frozen", v.ConfigFrozen)
	put("runtime/requests", v.Requests)
	put("runtime/failures", v.Failures)
	for _, s := range v.Sessions {
		put("session/"+stateKey(s.Key), s)
	}
	for _, r := range v.Resources.Resources {
		put("resource/"+r.RecordKey(), r)
	}
	for _, o := range v.Resources.Operations {
		put("operation/"+o.RecordKey(), o)
	}
	for _, x := range v.Tasks.Snapshots {
		put("tasks/"+x.Key, x)
	}
	for _, x := range v.SessionLists.Snapshots {
		put("session_lists/"+x.Key, x)
	}
	for key, ep := range v.Endpoints {
		ep.Proxy.Active, ep.A2A.Active = 0, 0
		put("endpoint/proxy/"+key, ep.Proxy)
		put("endpoint/a2a/"+key, ep.A2A)
	}
	for key, weights := range v.Schedules {
		put("schedule/"+key, weights)
	}
	for _, lease := range v.Transfer.Leases {
		put("lease/"+lease.ID, lease)
	}
	for i, receipt := range v.Transfer.Receipts {
		put("receipt/"+receipt.Key, shared.OrderedReceipt{Order: uint64(i + 1), Receipt: receipt})
	}
	for key, floor := range v.Transfer.Retired {
		put("retired/"+key, floor)
	}
	return records, nil
}

// RenderStateRecords materializes a final image once. Occupancy is rebuilt
// from lease descriptors; independently recorded session/resource counts are
// still checked by OpenStateView before the image can authorize ownership.
func RenderStateRecords(records map[string]json.RawMessage, epoch string, revision uint64, maxBytes int) ([]byte, string, error) {
	v := stateImage{Format: stateImageFormat, Sessions: []sessionImage{},
		Resources: resourcedir.Image{Resources: []resourcedir.ResourceImage{}, Operations: []resourcedir.OperationImage{}},
		Tasks:     shared.ListImage{Snapshots: []shared.ListEntry{}}, SessionLists: shared.ListImage{Snapshots: []shared.ListEntry{}},
		Endpoints: map[string]endpointImage{}, Schedules: map[string]map[string]int64{}, Callers: map[string]int64{},
		Transfer: shared.Transfer{Epoch: epoch, Revision: revision}}
	var receipts []shared.OrderedReceipt
	found := map[string]bool{}
	for key, raw := range records {
		var err error
		decode := func(value any) { err = json.Unmarshal(raw, value) }
		switch {
		case key == "runtime/config":
			decode(&v.Config)
			found[key] = true
		case key == "runtime/legacy":
			decode(&v.LegacyConfig)
			found[key] = true
		case key == "runtime/frozen":
			decode(&v.ConfigFrozen)
			found[key] = true
		case key == "runtime/requests":
			decode(&v.Requests)
			found[key] = true
		case key == "runtime/failures":
			decode(&v.Failures)
			found[key] = true
		case strings.HasPrefix(key, "session/"):
			var s sessionImage
			decode(&s)
			if key != "session/"+stateKey(s.Key) {
				return nil, "", fmt.Errorf("OPERATION_SESSION_KEY_MISMATCH")
			}
			v.Sessions = append(v.Sessions, s)
		case strings.HasPrefix(key, "resource/"):
			var r resourcedir.ResourceImage
			decode(&r)
			if key != "resource/"+r.RecordKey() {
				return nil, "", fmt.Errorf("OPERATION_RESOURCE_KEY_MISMATCH")
			}
			v.Resources.Resources = append(v.Resources.Resources, r)
		case strings.HasPrefix(key, "operation/"):
			var o resourcedir.OperationImage
			decode(&o)
			if key != "operation/"+o.RecordKey() {
				return nil, "", fmt.Errorf("OPERATION_RESERVATION_KEY_MISMATCH")
			}
			v.Resources.Operations = append(v.Resources.Operations, o)
		case strings.HasPrefix(key, "tasks/"), strings.HasPrefix(key, "session_lists/"):
			var x shared.ListEntry
			decode(&x)
			kind, name, _ := strings.Cut(key, "/")
			if name != x.Key {
				return nil, "", fmt.Errorf("OPERATION_CACHE_KEY_MISMATCH")
			}
			if kind == "tasks" {
				v.Tasks.Snapshots = append(v.Tasks.Snapshots, x)
			} else {
				v.SessionLists.Snapshots = append(v.SessionLists.Snapshots, x)
			}
		case strings.HasPrefix(key, "endpoint/"):
			kind, name, _ := strings.Cut(strings.TrimPrefix(key, "endpoint/"), "/")
			var iface interfaceImage
			decode(&iface)
			if iface.Active != 0 || (kind != "proxy" && kind != "a2a") {
				return nil, "", fmt.Errorf("INVALID_OPERATION_ENDPOINT")
			}
			ep := v.Endpoints[name]
			if kind == "proxy" {
				ep.Proxy = iface
			} else {
				ep.A2A = iface
			}
			v.Endpoints[name] = ep
		case strings.HasPrefix(key, "schedule/"):
			var weights map[string]int64
			decode(&weights)
			v.Schedules[strings.TrimPrefix(key, "schedule/")] = weights
		case strings.HasPrefix(key, "lease/"):
			var lease shared.LeaseRecord
			decode(&lease)
			if key != "lease/"+lease.ID {
				return nil, "", fmt.Errorf("OPERATION_LEASE_KEY_MISMATCH")
			}
			v.Transfer.Leases = append(v.Transfer.Leases, lease)
		case strings.HasPrefix(key, "receipt/"):
			var receipt shared.OrderedReceipt
			decode(&receipt)
			if key != "receipt/"+receipt.Receipt.Key || receipt.Order == 0 {
				return nil, "", fmt.Errorf("OPERATION_RECEIPT_KEY_MISMATCH")
			}
			receipts = append(receipts, receipt)
		case strings.HasPrefix(key, "retired/"):
			if v.Transfer.Retired == nil {
				v.Transfer.Retired = map[string]uint64{}
			}
			var floor uint64
			decode(&floor)
			v.Transfer.Retired[strings.TrimPrefix(key, "retired/")] = floor
		case strings.HasPrefix(key, "entry/"), strings.HasPrefix(key, "release/"):
			// Other process domains are decoded by the process coordinator.
		default:
			return nil, "", fmt.Errorf("UNKNOWN_OPERATION_RECORD: %s", key)
		}
		if err != nil {
			return nil, "", err
		}
	}
	if len(found) != 5 {
		return nil, "", fmt.Errorf("INCOMPLETE_OPERATION_IMAGE")
	}
	sort.Slice(v.Sessions, func(i, j int) bool {
		a, b := v.Sessions[i].Key, v.Sessions[j].Key
		return cmp.Or(cmp.Compare(a.Owner, b.Owner), cmp.Compare(a.Route, b.Route), cmp.Compare(a.ID, b.ID), cmp.Compare(a.Interface, b.Interface)) < 0
	})
	resourcedir.SortImage(&v.Resources)
	sort.Slice(v.Tasks.Snapshots, func(i, j int) bool { return v.Tasks.Snapshots[i].Key < v.Tasks.Snapshots[j].Key })
	sort.Slice(v.SessionLists.Snapshots, func(i, j int) bool { return v.SessionLists.Snapshots[i].Key < v.SessionLists.Snapshots[j].Key })
	sort.Slice(v.Transfer.Leases, func(i, j int) bool { return v.Transfer.Leases[i].ID < v.Transfer.Leases[j].ID })
	sort.Slice(receipts, func(i, j int) bool { return receipts[i].Order < receipts[j].Order })
	for i, r := range receipts {
		if i > 0 && receipts[i-1].Order == r.Order {
			return nil, "", fmt.Errorf("DUPLICATE_OPERATION_RECEIPT_ORDER")
		}
		v.Transfer.Receipts = append(v.Transfer.Receipts, r.Receipt)
	}
	routes := map[string]protocol.Route{}
	for _, route := range v.Config.Routes {
		routes[route.Key] = route
	}
	for _, lease := range v.Transfer.Leases {
		switch lease.Kind {
		case "acquire":
			route, ok := routes[lease.Route]
			if !ok {
				return nil, "", fmt.Errorf("OPERATION_LEASE_ROUTE_MISSING")
			}
			v.Inflight++
			v.Callers[lease.Owner]++
			key := route.ID + "/" + lease.Endpoint
			ep, ok := v.Endpoints[key]
			if !ok {
				return nil, "", fmt.Errorf("OPERATION_LEASE_ENDPOINT_MISSING")
			}
			ep.Active++
			if lease.Entry == "proxy" {
				ep.Proxy.Active++
			} else {
				ep.A2A.Active++
			}
			v.Endpoints[key] = ep
		case "cache_acquire":
			if lease.Entry == "tasks" {
				v.Tasks.Active++
			} else {
				v.SessionLists.Active++
			}
		}
	}
	v.Bindings = int64(len(v.Sessions))
	return shared.EncodeTransfer(v, maxBytes)
}

func (g *Gateway) SetRecorder(recorder *state.Recorder) {
	g.recorder = recorder
	g.stateBarrier.SetRecorder(recorder)
	g.resources.SetRecorder(recorder)
	g.taskLists.SetRecorder(recorder, "tasks/")
	g.sessionLists.SetRecorder(recorder, "session_lists/")
	g.dispatcher.SetRecorder(recorder)
}

func (g *Gateway) recordSessionLocked(key sessionKey, b *binding) {
	if !g.recorder.Active() {
		return
	}
	if b == nil {
		g.recorder.Delete("session/" + stateKey(key))
		return
	}
	g.recorder.Put("session/"+stateKey(key), sessionImage{key, b.Endpoint, b.WorkerOwner, b.Seen, b.TTL, b.Active})
}

func (g *Gateway) Recorder() *state.Recorder { return g.recorder }

func (g *Gateway) StopRecording() { g.stateBarrier.StopRecording() }

func (g *Gateway) recordInterface(key string, st *interfaceState, kind string) {
	if !g.recorder.Active() {
		return
	}
	v := captureInterface(st)
	v.Active = 0
	g.recorder.Put("endpoint/"+kind+"/"+key, v)
}

func (g *Gateway) setHealth(route, endpoint, kind string, healthy bool) {
	st := g.state(route, endpoint).iface(kind)
	st.discoveryMu.Lock()
	defer st.discoveryMu.Unlock()
	st.healthy.Store(healthy)
	g.recordInterface(route+"/"+endpoint, st, kind)
}
