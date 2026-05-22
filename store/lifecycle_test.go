package store

import (
	"os"
	"testing"
	"time"
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
	err = Open(DefaultConfig())
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

	err = Open(DefaultConfig())
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
	err = Open(DefaultConfig())
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

func TestRecoverAfterDeleteThenSet(t *testing.T) {
	setupTestStore(t)

	key := "key"
	oldValue := "value"
	newValue := "new_value"

	err := Set(key, oldValue, 0)
	if err != nil {
		t.Fatalf("set old value err: %v", err)
	}

	err = Del(key)
	if err != nil {
		t.Fatalf("del err: %v", err)
	}

	got, ok := Get(key)
	if ok {
		t.Fatalf("after del, get ok = true, got %q, want false", got)
	}

	err = Set(key, newValue, 0)
	if err != nil {
		t.Fatalf("set new value err: %v", err)
	}

	if err := Close(); err != nil {
		t.Fatalf("close err: %v", err)
	}

	err = Open(DefaultConfig())
	if err != nil {
		t.Fatalf("reopen err: %v", err)
	}

	got, ok = Get(key)
	if !ok {
		t.Fatalf("get ok = false, want true")
	}
	if got != newValue {
		t.Fatalf("got %q, want %q", got, newValue)
	}
}

func TestSetGetValueWithSpace(t *testing.T) {
	setupTestStore(t)

	key := "key"
	value := "你好 miniKV\nhello world"

	err := Set(key, value, 0)
	if err != nil {
		t.Fatalf("set err: %v", err)
	}

	got, ok := Get(key)
	if !ok {
		t.Fatalf("get err")
	}
	if got != value {
		t.Fatalf("got %v, want %v", got, value)
	}
}

func TestGetExpiredKey(t *testing.T) {
	setupTestStore(t)

	err := SetWithExpireAt("key", "value", time.Now().Unix()-1)
	if err != nil {
		t.Fatalf("set err: %v", err)
	}

	got, ok := Get("key")
	if ok {
		t.Fatalf("get ok = %v, want false", got)
	}
	if got != "" {
		t.Fatalf("got %q, want \"\"", got)
	}
}

func TestOpenFallBackToLogWhenHintMissing(t *testing.T) {
	setupTestStore(t)
	err := Set("key", "value", 0)
	if err != nil {
		t.Fatalf("set err: %v", err)
	}
	if err := Close(); err != nil {
		t.Fatalf("close err: %v", err)
	}

	if err := os.Remove("nosql.hint"); err != nil && !os.IsNotExist(err) {
		t.Fatalf("delete err: %v", err)
	}
	if err := Open(DefaultConfig()); err != nil {
		t.Fatalf("reopen err: %v", err)
	}
	got, ok := Get("key")
	if !ok {
		t.Fatalf("get ok = false, want true")
	}
	if got != "value" {
		t.Fatalf("got %q, want \"value\"", got)
	}
}
