package store

import (
	"os"
	"testing"
)

func TestSetThenGet(t *testing.T) {
	setupTestStore(t)
	err := Set("key", "value", 0)
	if err != nil {
		t.Fatalf("set err: %v", err)
	}
	want := "value"

	got, ok := Get("key")
	if !ok {
		t.Fatalf("get ok = false, want true")
	}
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}

}
func TestGetMissingKey(t *testing.T) {
	setupTestStore(t)

	got, ok := Get("key")
	if ok {
		t.Fatalf("Get ok = true, want false")
	}
	if got != "" {
		t.Fatalf("got %q, want \"\"", got)
	}

}

func TestDelThenGetMissing(t *testing.T) {
	setupTestStore(t)
	err := Set("key", "value", 0)
	if err != nil {
		t.Fatalf("set err: %v", err)
	}

	err = Del("key")
	if err != nil {
		t.Fatalf("del err: %v", err)
	}

	got, ok := Get("key")
	if ok {
		t.Fatalf("Get ok = true ,want false")
	}
	if got != "" {
		t.Fatalf("got %v, want \"\"", got)
	}

}
func TestSetOverwrite(t *testing.T) {
	setupTestStore(t)
	oldValue := "value1"
	newValue := "value2"
	err := Set("key", oldValue, 0)
	if err != nil {
		t.Fatalf("set err: %v", err)
	}

	get1, ok := Get("key")
	if !ok {
		t.Fatalf("get ok = false, want true")
	}
	if get1 != oldValue {
		t.Fatalf("got %q, want %q", get1, oldValue)
	}

	err = Set("key", newValue, 0)
	if err != nil {
		t.Fatalf("set err: %v", err)
	}

	get2, ok := Get("key")
	if !ok {
		t.Fatalf("get ok = false, want true")
	}
	if get2 != newValue {
		t.Fatalf("got %q, want %q", get2, newValue)
	}
}
func TestRecoverAfterSet(t *testing.T) {
	setupTestStore(t)
	value := "value"
	err := Set("key", value, 0)
	if err != nil {
		t.Fatalf("set err: %v", err)
	}
	if err := Close(); err != nil {
		t.Fatalf("close err: %v", err)
	}
	err = Open("nosql.json")
	if err != nil {
		t.Fatalf("reopen err: %v", err)
	}
	got, ok := Get("key")
	if !ok {
		t.Fatalf("get ok = false, want true")
	}
	if got != value {
		t.Fatalf("got %q, want %q", got, value)
	}
}

func TestRecoverAfterOverwrite(t *testing.T) {
	setupTestStore(t)
	oldValue := "value1"
	newValue := "value2"
	err := Set("key", oldValue, 0)
	if err != nil {
		t.Fatalf("set err: %v", err)
	}
	err = Set("key", newValue, 0)
	if err != nil {
		t.Fatalf("set err: %v", err)
	}
	if err := Close(); err != nil {
		t.Fatalf("close err: %v", err)
	}

	err = Open("nosql.json")
	if err != nil {
		t.Fatalf("reopen err: %v", err)
	}
	got, ok := Get("key")
	if !ok {
		t.Fatalf("get ok = false, want true")
	}
	if got != newValue {
		t.Fatalf("got %q, want %q", got, newValue)
	}
}

func TestRecoverAfterDelete(t *testing.T) {
	setupTestStore(t)
	key := "key"
	value := "value"
	err := Set(key, value, 0)
	if err != nil {
		t.Fatalf("set err: %v", err)
	}
	err = Del(key)
	if err != nil {
		t.Fatalf("del err: %v", err)
	}
	got, ok := Get(key)
	if ok {
		t.Fatalf("after del, get ok = true, got %q, want false", got)
	}
	if err := Close(); err != nil {
		t.Fatalf("close err: %v", err)
	}
	err = Open("nosql.json")
	if err != nil {
		t.Fatalf("reopen err: %v", err)
	}
	got, ok = Get(key)
	if ok {
		t.Fatalf("after reopen, get ok = true, got %q, want false", got)
	}
	if got != "" {
		t.Fatalf("got %q, want \"\"", got)
	}
}

func setupTestStore(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	oldWd, err := os.Getwd()
	if err != nil {
		t.Fatalf("os.Getwd: %v", err)
	}

	err = os.Chdir(dir)
	if err != nil {
		t.Fatalf("chdir err: %v", err)
	}
	t.Cleanup(func() {
		if err := Close(); err != nil {
			t.Errorf("close err: %v", err)
		}
		_ = os.Chdir(oldWd)
	})
	err = Open("nosql.json")
	if err != nil {
		t.Fatalf("open err: %v", err)
	}
}
