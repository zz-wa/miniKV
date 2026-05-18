package store

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

var Shards [16]*Shard

var isCompaction atomic.Bool
var lruCache *LRUCache

var writeFile *os.File
var readFile *os.File
var fileMu sync.RWMutex

func Open(filename string) error {
	for i := 0; i < 16; i++ {
		Shards[i] = NewShard()
	}
	recoverPendingCompaction()

	lruCache = NewLRUCache(1000)

	switch hintIsComplete("nosql.hint") {
	case true:
		loadIndexFromHint("nosql.hint")
	case false:
		err := rebuildIndexFromLog(filename)
		if err != nil {
			return err
		}
	}

	wf, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	rf, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		closeErr := wf.Close()
		if closeErr != nil {
			return errors.Join(err, closeErr)
		}
		return err
	}
	writeFile = wf
	readFile = rf

	go CleanupExpired()
	return nil

}

func Set(key, value string, ttl int64) error {
	var expireAt int64
	if ttl > 0 {
		expireAt = time.Now().Unix() + ttl
	}
	return setInternal(key, value, expireAt)
}

func setInternal(key, value string, expiredAt int64) error {
	if err := validateKey(key); err != nil {
		return err
	}
	shard := GetShard(key)
	fileMu.Lock()
	defer fileMu.Unlock()
	shard.Lock()
	defer shard.Unlock()

	record := NewSetRecord(key, value, expiredAt)
	offset, bodayLen, err := appendRecord(record)
	if err != nil {
		return err
	}
	shard.index[key] = Entry{
		Offset:   offset,
		Length:   bodayLen,
		ExpireAt: expiredAt,
	}
	lruCache.Put(key, value)
	go Compaction("nosql.json")
	return nil
}

func Get(key string) (string, bool) {
	if err := validateKey(key); err != nil {
		return "", false
	}
	fileMu.RLock()

	shard := GetShard(key)

	shard.RLock()
	en, ok := shard.index[key]
	if !ok {
		shard.RUnlock()
		fileMu.RUnlock()

		return "", false
	}
	if en.ExpireAt != 0 && time.Now().Unix() > en.ExpireAt {
		fileMu.RUnlock()

		shard.RUnlock()
		deleteExpired(key)
		return "", false
	}
	buf := make([]byte, lengthPrefixSize+en.Length)
	shard.RUnlock()

	_, err := readFile.ReadAt(buf, int64(en.Offset))
	if err != nil && err != io.EOF {
		fileMu.RUnlock()
		return "", false
	}

	record, err := decodeRecord(buf)
	if err != nil && err != io.EOF {
		fileMu.RUnlock()
		return "", false
	}
	if record.Op != OpSet || record.Key != key {
		fileMu.RUnlock()
		return "", false
	}

	lruCache.Get(key)
	fileMu.RUnlock()
	return record.Value, true
}

func Del(key string) error {
	if err := validateKey(key); err != nil {
		return err
	}
	fileMu.Lock()
	defer fileMu.Unlock()

	shard := GetShard(key)

	shard.Lock()
	defer shard.Unlock()
	err := delLocked(shard, key)
	if err != nil {
		return err
	}
	return nil
}

func delLocked(shard *Shard, key string) error {
	record := NewDelRecord(key)

	_, _, err := appendRecord(record)
	if err != nil {
		return err
	}
	delete(shard.index, key)

	lruCache.Remove(key)
	go Compaction("nosql.json")
	return nil

}

func deleteExpired(key string) {
	fileMu.Lock()
	defer fileMu.Unlock()

	shard := GetShard(key)

	shard.Lock()
	defer shard.Unlock()
	en, ok := shard.index[key]
	if !ok {
		return
	}
	if en.ExpireAt != 0 && en.ExpireAt < time.Now().Unix() {
		record := NewDelRecord(key)
		_, _, err := appendRecord(record)
		if err != nil {
			return
		}
		lruCache.Remove(key)
		delete(shard.index, key)
	}
	return
}

func GetMeta(key string) (value string, expiredAt int64, exists bool) {
	if err := validateKey(key); err != nil {
		return "", 0, false
	}
	fileMu.RLock()
	defer fileMu.RUnlock()

	shard := GetShard(key)

	shard.RLock()
	defer shard.RUnlock()
	en, ok := shard.index[key]
	if !ok {
		return "", 0, false
	}
	buf := make([]byte, lengthPrefixSize+en.Length)

	_, err := readFile.ReadAt(buf, en.Offset)
	if err != nil {
		return "", 0, false
	}

	record, err := decodeRecord(buf)
	if err != nil && err != io.EOF {
		return "", 0, false
	}
	if record.Op != OpSet || record.Key != key {
		return "", 0, false
	}
	if record.ExpireAt != 0 && time.Now().Unix() > record.ExpireAt {
		return "", 0, false
	}

	return record.Value, record.ExpireAt, true
}

func SetWithExpireAt(key, value string, expiredAt int64) error {
	return setInternal(key, value, expiredAt)
}

/*
func tmpIsComplete(filename string) bool {
	data, _ := os.ReadFile(filename)
	offset := 0
	for offset < len(data) {
		if offset+4 > len(data) {
			break
		}
		bodyLen := int(binary.BigEndian.Uint32(data[offset : offset+4]))
		if offset+4+bodyLen > len(data) {
			break
		}
		body := string(data[offset+4 : offset+4+bodyLen])
		l := strings.SplitN(body, " ", 4)
		if len(l) >= 1 && l[0] == "DONE" {
			return true
		}
		offset += 4 + bodyLen
	}
	return false
}
*/

func hintIsComplete(filename string) bool {
	data, _ := os.ReadFile(filename)
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		if line == "DONE" {
			return true
		}
	}
	return false
}

func recoverPendingCompaction() {
	_, err := os.Stat("nosql.tmp")

	if err == nil {
		err := os.Remove("nosql.tmp")
		if err != nil {
			zap.S().Fatal("remove nosql.tmp fail")
		}
	}
}

func loadIndexFromHint(filename string) {
	hintData, _ := os.ReadFile(filename)
	for _, line := range strings.Split(string(hintData), "\n") {
		l := strings.SplitN(line, " ", 4)
		if len(l) < 4 {
			continue
		}

		offset, err := strconv.ParseInt(l[1], 10, 64)
		if err != nil {
			zap.S().Fatal("parse offset fail")
		}
		length, err := strconv.ParseInt(l[2], 10, 64)
		if err != nil {
			zap.S().Fatal("parse length fail")
		}
		expireAt, err := strconv.ParseInt(l[3], 10, 64)
		if err != nil {
			zap.S().Fatal("parse expireAt fail")
		}
		shard := GetShard(l[0])

		shard.index[l[0]] = Entry{
			Offset:   offset,
			Length:   length,
			ExpireAt: expireAt,
		}
	}
}

func rebuildIndexFromLog(filename string) error {
	err := os.Remove("nosql.hint")
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	data, _ := os.ReadFile(filename)
	offset := 0
	for offset < len(data) {
		if offset+lengthPrefixSize > len(data) {
			break
		}
		bodyLen := int(binary.BigEndian.Uint32(data[offset : offset+lengthPrefixSize]))
		end := offset + lengthPrefixSize + bodyLen
		if end > len(data) {
			break
		}

		record, err := decodeRecord(data[offset:end])
		if err != nil {
			break
		}

		switch record.Op {
		case OpSet:
			if record.ExpireAt == 0 || time.Now().Unix() <= record.ExpireAt {
				shard := GetShard(record.Key)
				shard.index[record.Key] = Entry{
					Offset:   int64(offset),
					Length:   int64(bodyLen),
					ExpireAt: record.ExpireAt,
				}
			}
		case OpDel:
			shard := GetShard(record.Key)
			delete(shard.index, record.Key)
		}
		offset = end
	}
	return nil
}

func Close() error {
	fileMu.Lock()
	defer fileMu.Unlock()

	var err error
	if writeFile != nil {
		err = errors.Join(err, writeFile.Close())
		writeFile = nil
	}

	if readFile != nil {
		err = errors.Join(err, readFile.Close())
		readFile = nil
	}
	return err
}

func appendRecord(record Record) (offset int64, length int64, err error) {
	data := encodeRecord(record)

	offset, err = writeFile.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, 0, err
	}
	_, err = writeFile.Write(data)
	if err != nil {
		return 0, 0, err
	}
	return offset, int64(recordBodyLen(record)), nil
}

func validateKey(key string) error {
	if key == "" {
		return errors.New("key is empty")
	}

	if strings.ContainsAny(key, " \t\r\n") {
		return errors.New("invalid key")
	}
	return nil
}
