package api

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/prananallari/mini-kafka/internal/broker"
)

type Handler struct {
	s *broker.Service
}

func NewHandler(s *broker.Service) *Handler {
	return &Handler{s: s}
}

func (h *Handler) CreateTopic(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name           string `json:"name"`
		PartitionCount int    `json:"partition_count"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	if req.Name == "" || req.PartitionCount <= 0 {
		http.Error(w, "invalid input", http.StatusBadRequest)
		return
	}

	if err := h.s.CreateTopic(r.Context(), req.Name, req.PartitionCount); err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
}

func (h *Handler) CreateGroup(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name string `json:"name"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	if req.Name == "" {
		http.Error(w, "invalid input", http.StatusBadRequest)
		return
	}

	if err := h.s.CreateGroup(r.Context(), req.Name); err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
}

func (h *Handler) Publish(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("name")
	var req struct {
		Key     string `json:"key"`
		Payload string `json:"payload"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	if req.Key == "" || req.Payload == "" {
		http.Error(w, "invalid input", http.StatusBadRequest)
		return
	}

	if err := h.s.Publish(r.Context(), topic, req.Key, []byte(req.Payload)); err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
}

func (h *Handler) Consume(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("name")
	group := r.URL.Query().Get("group")
	partitionStr := r.URL.Query().Get("partition")
	limitStr := r.URL.Query().Get("limit")

	if partitionStr == "" || limitStr == "" || topic == "" || group == "" {
		http.Error(w, "invalid details", http.StatusBadRequest)
		return
	}

	partition, err := strconv.Atoi(partitionStr)
	if err != nil {
		http.Error(w, "invalid partition", http.StatusBadRequest)
		return
	}

	limit, err := strconv.Atoi(limitStr)
	if err != nil {
		http.Error(w, "invalid limit", http.StatusBadRequest)
		return
	}

	if partition < 0 || limit <= 0 {
		http.Error(w, "invalid details", http.StatusBadRequest)
		return
	}

	data, err := h.s.Consume(r.Context(), topic, group, partition, limit)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	resp := make([]string, len(data))
	for i, v := range data {
		resp[i] = string(v)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}
