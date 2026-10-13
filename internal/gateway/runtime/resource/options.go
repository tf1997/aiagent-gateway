package resource

type Limits struct {
	MaxResources, MaxResourceAliases, MaxDataIdempotency int
	MaxSessions, MaxTasks                                int
}

func (d *Directory) SetFailureHandler(handler func()) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.onFailure = handler
}
