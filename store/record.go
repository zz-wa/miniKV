package store

import (
	"encoding/binary"
	"errors"
)

type OpType byte

const (
	OpSet OpType = 1
	OpDel OpType = 2
)

const (
	lengthPrefixSize = 4

	opTypeSize   = 1
	keyLenSize   = 4
	valueLenSize = 4
	expireAtSize = 8

	recordHeaderSize = opTypeSize + keyLenSize + valueLenSize + expireAtSize
)

type Record struct {
	Op       OpType
	Key      string
	Value    string
	ExpireAt int64
}

func NewSetRecord(key, value string, expireAt int64) Record {
	return Record{
		Op:       OpSet,
		Key:      key,
		Value:    value,
		ExpireAt: expireAt,
	}
}

func NewDelRecord(key string) Record {
	return Record{
		Op:       OpDel,
		Key:      key,
		Value:    "",
		ExpireAt: 0,
	}
}

func recordBodyLen(r Record) int {
	keyBytes := []byte(r.Key)
	valueBytes := []byte(r.Value)

	if r.Op == OpDel {
		valueBytes = nil
	}
	return recordHeaderSize + len(keyBytes) + len(valueBytes)
}

func encodeRecord(r Record) []byte {

	keyBytes := []byte(r.Key)
	valueBytes := []byte(r.Value)

	if r.Op == OpDel {
		valueBytes = nil
	}

	bodyLen := recordBodyLen(r)

	totalLen := lengthPrefixSize + bodyLen

	data := make([]byte, totalLen)
	binary.BigEndian.PutUint32(data[0:4], uint32(bodyLen))

	body := data[4:]
	body[0] = byte(r.Op)
	binary.BigEndian.PutUint32(body[1:5], uint32(len(keyBytes)))
	binary.BigEndian.PutUint32(body[5:9], uint32(len(valueBytes)))
	binary.BigEndian.PutUint64(body[9:17], uint64(r.ExpireAt))

	keyStart := recordHeaderSize
	keyEnd := keyStart + len(keyBytes)

	copy(body[keyStart:keyEnd], keyBytes)
	copy(body[keyEnd:], valueBytes)

	return data
}

func decodeRecord(data []byte) (Record, error) {
	if len(data) < lengthPrefixSize {
		return Record{}, errors.New("invalid record")
	}

	bodyLen := int(binary.BigEndian.Uint32(data[0:4]))

	body := data[4:]

	if len(body) != bodyLen {
		return Record{}, errors.New("invalid record length")
	}

	if len(body) < recordHeaderSize {
		return Record{}, errors.New("record body too short")
	}

	op := OpType(body[0])

	if op != OpSet && op != OpDel {
		return Record{}, errors.New("invalid op")
	}

	keyLen := int(binary.BigEndian.Uint32(body[1:5]))
	valueLen := int(binary.BigEndian.Uint32(body[5:9]))
	expiredAt := int64(binary.BigEndian.Uint64(body[9:17]))

	expectedLen := recordHeaderSize + keyLen + valueLen
	if len(body) != expectedLen {
		return Record{}, errors.New("invalid record length")
	}

	keyStart := recordHeaderSize
	keyEnd := keyLen + keyStart
	valueStart := keyEnd
	valueEnd := valueStart + valueLen

	keyBytes := body[keyStart:keyEnd]
	valueBytes := body[valueStart:valueEnd]

	return Record{
		Op:       op,
		Key:      string(keyBytes),
		Value:    string(valueBytes),
		ExpireAt: expiredAt,
	}, nil

}
