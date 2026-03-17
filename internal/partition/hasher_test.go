package partition

import "testing"

func TestGetHash_Deterministic(t *testing.T) {
	if GetHash("same-key") != GetHash("same-key") {
		t.Error("expected same hash for same input")
	}
}

func TestGetHash_DifferentKeys(t *testing.T) {
	if GetHash("key-a") == GetHash("key-b") {
		t.Error("expected different hashes for different inputs")
	}
}

func TestGetHash_EmptyString(t *testing.T) {
	hash := GetHash("")
	if hash == 0 {
		t.Error("expected non-zero hash for empty string")
	}
}
