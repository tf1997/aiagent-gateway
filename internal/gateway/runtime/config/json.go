package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
)

func decode(b []byte, v any) error {
	d := json.NewDecoder(strings.NewReader(string(b)))
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		var typeError *json.UnmarshalTypeError
		if errors.As(err, &typeError) && (typeError.Field == "token" || strings.HasSuffix(typeError.Field, ".token")) {
			return fmt.Errorf("invalid upstream token: expected a string")
		}
		return err
	}
	if d.Decode(new(any)) != io.EOF {
		return fmt.Errorf("invalid trailing JSON")
	}
	return nil
}

func merge(b []byte, v any) error {
	old, _ := json.Marshal(v)
	var dst, patch map[string]any
	if json.Unmarshal(old, &dst) != nil || json.Unmarshal(b, &patch) != nil || patch == nil {
		return fmt.Errorf("invalid patch")
	}
	mergeMap(dst, patch)
	out, _ := json.Marshal(dst)
	fresh := reflect.New(reflect.TypeOf(v).Elem())
	if err := decode(out, fresh.Interface()); err != nil {
		return err
	}
	reflect.ValueOf(v).Elem().Set(fresh.Elem())
	return nil
}

func mergeMap(dst, patch map[string]any) {
	for k, v := range patch {
		if v == nil {
			delete(dst, k)
			continue
		}
		if m, ok := v.(map[string]any); ok {
			base, ok := dst[k].(map[string]any)
			if !ok {
				base = map[string]any{}
			}
			mergeMap(base, m)
			dst[k] = base
		} else {
			dst[k] = v
		}
	}
}
