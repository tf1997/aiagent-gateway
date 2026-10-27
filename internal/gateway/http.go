package gateway

import (
	"net/http"
	"strings"
	"time"
)

// PublicHandler serves administration and agent traffic on one listener.
// Management requests keep their own authentication and bounded I/O; agent
// streams must not inherit the management response deadline.
func PublicHandler(data, admin http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" || r.URL.Path == "/admin" || strings.HasPrefix(r.URL.Path, "/admin/") {
			controller := http.NewResponseController(w)
			deadline := time.Now().Add(15 * time.Second)
			_ = controller.SetReadDeadline(deadline)
			_ = controller.SetWriteDeadline(deadline)
			defer controller.SetReadDeadline(time.Time{})
			defer controller.SetWriteDeadline(time.Time{})
			admin.ServeHTTP(w, r)
			return
		}
		data.ServeHTTP(w, r)
	})
}
