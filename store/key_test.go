package store

import "testing"

func TestSetRejectsInvalidKey(t *testing.T) {
	key := "k ey"
	value := "v"
	err := Set(key, value, 0)
	if err == nil {
		t.Fatal("Set err = nil, want error")
	}
}

func TestDelRejectsInvalidKey(t *testing.T) {
	key := "k ey"
	err := Del(key)
	if err == nil {
		t.Fatal("Del err = nil, want error")
	}
}

func TestGetInvalidKeyReturnsMissing(t *testing.T) {
	key := "k ey"
	got, ok := Get(key)
	if ok {
		t.Fatalf("Get ok = true, got %q, want false", got)
	}
	if got != "" {
		t.Fatalf("got %q, want \"\"", got)
	}

}
