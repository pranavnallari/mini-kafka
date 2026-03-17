package api

import "net/http"

func NewRouter(h *Handler) *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /topics", h.CreateTopic)
	mux.HandleFunc("POST /groups", h.CreateGroup)
	mux.HandleFunc("POST /topics/{name}/messages", h.Publish)
	mux.HandleFunc("GET /topics/{name}/messages", h.Consume)
	mux.HandleFunc("POST /topics/{name}/messages/retry", h.RetryMessages)
	return mux
}
