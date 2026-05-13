package store

import (
	"encoding/binary"
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

	err = Open("nosql.json")
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

func TestRecordBodyLen(t *testing.T) {
	key := "name"
	value := "waa"

	setRecord := NewSetRecord(key, value, 0)
	delRecord := NewDelRecord(key)
	gotSet := recordBodyLen(setRecord)
	gotDel := recordBodyLen(delRecord)
	if gotSet != 24 {
		t.Fatalf("set got %v, want %v", gotSet, 24)
	}
	if gotDel != 21 {
		t.Fatalf("del got %v, want %v", gotDel, 21)
	}
}

func TestEncodeRecordLayout(t *testing.T) {
	tests := []struct {
		name         string
		record       Record
		wantBodyLen  uint32
		wantOp       OpType
		wantKeyLen   uint32
		wantValueLen uint32
		wantExpireAt uint64
		wantKey      string
		wantValue    string
	}{
		{
			name:         "set record",
			record:       NewSetRecord("name", "waa", 0),
			wantBodyLen:  24,
			wantOp:       OpSet,
			wantKeyLen:   4,
			wantValueLen: 3,
			wantExpireAt: 0,
			wantKey:      "name",
			wantValue:    "waa",
		},
		{
			name:         "del record",
			record:       NewDelRecord("name"),
			wantBodyLen:  21,
			wantOp:       OpDel,
			wantKeyLen:   4,
			wantValueLen: 0,
			wantExpireAt: 0,
			wantKey:      "name",
			wantValue:    "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := encodeRecord(tt.record)
			gotBodyLen := binary.BigEndian.Uint32(data[0:4])
			if gotBodyLen != tt.wantBodyLen {
				t.Fatalf("got %v, want %v", gotBodyLen, tt.wantBodyLen)
			}
			if len(data) != lengthPrefixSize+int(tt.wantBodyLen) {
				t.Fatalf("len(data) = %d, want %d", len(data), lengthPrefixSize+int(tt.wantBodyLen))
			}

			body := data[4:]

			gotOp := OpType(body[0])
			if gotOp != tt.wantOp {
				t.Fatalf("op = %v, want %v", gotOp, tt.wantOp)
			}

			gotKeyLen := binary.BigEndian.Uint32(body[1:5])
			if gotKeyLen != tt.wantKeyLen {
				t.Fatalf("keyLen = %d, want %d", gotKeyLen, tt.wantKeyLen)
			}

			gotValueLen := binary.BigEndian.Uint32(body[5:9])
			if gotValueLen != tt.wantValueLen {
				t.Fatalf("valueLen = %d, want %d", gotValueLen, tt.wantValueLen)
			}

			gotExpireAt := binary.BigEndian.Uint64(body[9:17])
			if gotExpireAt != tt.wantExpireAt {
				t.Fatalf("expireAt = %d, want %d", gotExpireAt, tt.wantExpireAt)
			}
			keyStart := recordHeaderSize
			keyEnd := keyStart + int(gotKeyLen)
			valueStart := keyEnd
			valueEnd := valueStart + int(gotValueLen)

			gotKey := string(body[keyStart:keyEnd])
			if gotKey != tt.wantKey {
				t.Fatalf("key = %q, want %q", gotKey, tt.wantKey)
			}

			gotValue := string(body[valueStart:valueEnd])
			if gotValue != tt.wantValue {
				t.Fatalf("value = %q, want %q", gotValue, tt.wantValue)
			}
		})
	}
}

func TestEncodeDecodeRecord(t *testing.T) {
	tests := []struct {
		name   string
		record Record
	}{
		{
			name:   "set record",
			record: NewSetRecord("name", "waa", 0),
		},
		{
			name:   "del record",
			record: NewDelRecord("name"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := encodeRecord(tt.record)
			got, err := decodeRecord(data)
			if err != nil {
				t.Fatalf("decode err: %v", err)
			}
			if got.Op != tt.record.Op {
				t.Fatalf("op = %v, want %v", got.Op, tt.record.Op)
			}
			if got.Key != tt.record.Key {
				t.Fatalf("key = %v, want %v", got.Key, tt.record.Key)
			}
			if got.Value != tt.record.Value {
				t.Fatalf("value = %v, want %v", got.Value, tt.record.Value)
			}
			if got.ExpireAt != tt.record.ExpireAt {
				t.Fatalf("expireAt = %v, want %v", got.ExpireAt, tt.record.ExpireAt)
			}
		})
	}
}
