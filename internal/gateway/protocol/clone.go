package protocol

import (
	"bytes"
	"maps"
	"slices"
)

// Clone detaches every writable collection, including the private endpoint
// index. It does not encode credentials, cards or receipts to copy them.
func (c Config) Clone() Config {
	c.Routes = slices.Clone(c.Routes)
	for i := range c.Routes {
		r := &c.Routes[i]
		r.endpointIndex = nil
		r.Expose = slices.Clone(r.Expose)
		r.Endpoints = slices.Clone(r.Endpoints)
		for j := range r.Endpoints {
			r.Endpoints[j].Interfaces = maps.Clone(r.Endpoints[j].Interfaces)
		}
		r.Consumers = slices.Clone(r.Consumers)
		r.AllowedMethods = slices.Clone(r.AllowedMethods)
		r.Card = bytes.Clone(r.Card)
	}
	c.Receipts = maps.Clone(c.Receipts)
	for k, r := range c.Receipts {
		r.Body = bytes.Clone(r.Body)
		c.Receipts[k] = r
	}
	c.ReceiptOrder = slices.Clone(c.ReceiptOrder)
	c.Audit = slices.Clone(c.Audit)
	return c
}
