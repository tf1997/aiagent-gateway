package entry

import (
	p "aiagent-gateway/internal/releaseprotocol"
	"encoding/json"
	"fmt"
	"strings"
)

func StateRecords(v Transfer) map[string]json.RawMessage {
	records := map[string]json.RawMessage{"entry/traffic": jsonValue(v.Traffic), "entry/counter": jsonValue(v.Counter), "entry/lost": jsonValue(v.Lost)}
	if v.Compact {
		records["entry/compact"] = jsonValue(true)
	}
	for k, value := range v.Receipts {
		records["entry/receipt/"+k] = jsonValue(value)
	}
	for k, value := range v.Bindings {
		records["entry/binding/"+k] = jsonValue(value)
	}
	for k, value := range v.Results {
		records["entry/result/"+k] = jsonValue(value)
	}
	for k, value := range v.Active {
		records["entry/active/"+k] = jsonValue(value)
	}
	return records
}

func RenderStateRecords(records map[string]json.RawMessage) (Transfer, error) {
	v := Transfer{Bindings: map[string]string{}, Results: map[string]p.Result{}, Active: map[string]p.Result{}, Receipts: map[string]ResultReceipt{}}
	found := 0
	for k, raw := range records {
		var err error
		switch {
		case k == "entry/traffic":
			err = json.Unmarshal(raw, &v.Traffic)
			found++
		case k == "entry/counter":
			err = json.Unmarshal(raw, &v.Counter)
			found++
		case k == "entry/lost":
			err = json.Unmarshal(raw, &v.Lost)
			found++
		case k == "entry/compact":
			err = json.Unmarshal(raw, &v.Compact)
		case strings.HasPrefix(k, "entry/receipt/"):
			var value ResultReceipt
			err = json.Unmarshal(raw, &value)
			v.Receipts[strings.TrimPrefix(k, "entry/receipt/")] = value
		case strings.HasPrefix(k, "entry/binding/"):
			var value string
			err = json.Unmarshal(raw, &value)
			v.Bindings[strings.TrimPrefix(k, "entry/binding/")] = value
		case strings.HasPrefix(k, "entry/result/"), strings.HasPrefix(k, "entry/active/"):
			var value p.Result
			err = json.Unmarshal(raw, &value)
			kind, key, _ := strings.Cut(strings.TrimPrefix(k, "entry/"), "/")
			if key != value.ID {
				return v, fmt.Errorf("OPERATION_ENTRY_KEY_MISMATCH")
			}
			if kind == "result" {
				v.Results[key] = value
			} else {
				v.Active[key] = value
			}
		case strings.HasPrefix(k, "entry/"):
			return v, fmt.Errorf("UNKNOWN_ENTRY_RECORD")
		}
		if err != nil {
			return v, err
		}
	}
	if found != 3 {
		return v, fmt.Errorf("INCOMPLETE_ENTRY_RECORDS")
	}
	return v, v.Validate()
}
