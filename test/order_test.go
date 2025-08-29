package main

import (
	"L_0/internal/models"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// ---------- UNIT TESTS ----------

func TestOrderValidate(t *testing.T) {
	validOrder := models.Order{
		OrderUID:    "123",
		TrackNumber: "TRACK123",
		Payment: models.Payment{
			Transaction: "TX123",
		},
		Items: []models.Item{{ChrtID: 1, Name: "Item1"}},
	}

	if err := validOrder.Validate(); err != nil {
		t.Errorf("expected valid order, got error %v", err)
	}

	invalidOrder := validOrder
	invalidOrder.OrderUID = ""
	if err := invalidOrder.Validate(); err == nil {
		t.Errorf("expected error for missing OrderUID")
	}
}

// ---------- HANDLER TESTS ----------

// Fake in-memory cache for testing
var testCache = map[string]models.Order{
	"known123": {
		OrderUID:    "known123",
		TrackNumber: "TRACK-XYZ",
		Payment:     models.Payment{Transaction: "TX987"},
		Items:       []models.Item{{ChrtID: 1, Name: "Test Item"}},
	},
}

// Mock handler to test GET /order/:uid/
func mockOrderHandler(w http.ResponseWriter, r *http.Request) {
	orderUid := r.PathValue("uid")
	order, ok := testCache[orderUid]
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	writeJSON(w, order)
}

func TestOrderHandler_Found(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/order/known123", nil)
	w := httptest.NewRecorder()

	mockOrderHandler(w, req)

	res := w.Result()
	if res.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", res.StatusCode)
	}
}

func TestOrderHandler_NotFound(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/order/unknown999", nil)
	w := httptest.NewRecorder()

	mockOrderHandler(w, req)

	res := w.Result()
	if res.StatusCode != http.StatusNotFound {
		t.Errorf("expected 404, got %d", res.StatusCode)
	}
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		http.Error(w, "failed to encode response", http.StatusInternalServerError)
	}
}
