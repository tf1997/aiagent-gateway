package resource

import (
	"aiagent-gateway/internal/state"
	"sort"
)

func (r ResourceImage) RecordKey() string { return stateKey(r.Resource.Key()) }
func (o OperationImage) RecordKey() string {
	return stateKey(operationKey{o.Owner, o.Route, o.Operation, o.Key})
}
func (d *Directory) SetRecorder(r *state.Recorder) { d.recorder = r }
func resourceImage(r *resourceRecord) ResourceImage {
	x := ResourceImage{r.Resource, r.fingerprint, append([]string{}, r.aliases...), append([]string{}, r.upstreams...), r.operationRefs}
	sort.Strings(x.Aliases)
	sort.Strings(x.Upstreams)
	return x
}

// Called before releasing the directory lock, including read operations that touch TTLs.
func (d *Directory) recordResource(r *resourceRecord) {
	if !d.recorder.Active() {
		return
	}
	d.recorder.Put("resource/"+stateKey(r.Key()), resourceImage(r))
}
func (d *Directory) recordOperation(k operationKey) {
	if !d.recorder.Active() {
		return
	}
	o := d.operations[k]
	d.recorder.Put("operation/"+stateKey(k), OperationImage{k.Owner, k.Route, k.Operation, k.Key, o.Resource, o.Fingerprint, o.Seen})
}
