package store

import (
	"encoding/binary"
	"testing"
)

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
