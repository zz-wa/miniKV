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
		_ = os.Chdir(oldWd)
	})
	err = Open("nosql.json")
	if err != nil {
		t.Fatalf("open err: %v", err)
	}
}
