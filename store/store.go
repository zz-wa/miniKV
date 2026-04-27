package store

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

var Shards [16]*Shard

var isCompaction atomic.Bool
var lruCache *LRUCache

var writeFile *os.File
var readFile *os.File

func Open(filename string) error {
	for i := 0; i < 16; i++ {
		Shards[i] = NewShard()
	}
	_, err := os.Stat("nosql.tmp")

	if err == nil {
		if tmpIsComplete("nosql.tmp") {
			os.Rename("nosql.tmp", filename)
		} else {
			os.Remove("nosql.tmp")
		}
	}

	lruCache = NewLRUCache(1000)

	switch HintIsComplete("nosql.hint") {
	case true:
		hintdata, _ := os.ReadFile("nosql.hint")
		for _, line := range strings.Split(string(hintdata), "\n") {
			l := strings.SplitN(line, " ", 4)
			if len(l) < 4 {
				continue
			}

			offset, _ := strconv.ParseInt(l[1], 10, 64)
			length, _ := strconv.ParseInt(l[2], 10, 64)
			expireAt, _ := strconv.ParseInt(l[3], 10, 64)
			shard := GetShard(l[0])

			shard.index[l[0]] = Entry{
				Offset:   offset,
				Length:   length,
				ExpireAt: expireAt,
			}
		}
	case false:
		os.Remove("nosql.hint")
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
			if len(l) >= 4 && l[0] == "set" {

				expireAt, err := strconv.ParseInt(strings.TrimSpace(l[3]), 10, 64)
				if err == nil && (expireAt == 0 || time.Now().Unix() <= expireAt) {
					shard := GetShard(l[0])
					shard.index[l[1]] = Entry{
						Offset:   int64(offset),
						Length:   int64(bodyLen),
						ExpireAt: expireAt,
					}
				}
			}
			if len(l) >= 2 && l[0] == "del" {
				shard := GetShard(l[0])

				delete(shard.index, strings.TrimSpace(l[1]))
			}
			if len(l) >= 1 && l[0] == "DONE" {
			}
			offset += 4 + bodyLen
		}
	}

	writeFile, _ = os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	readFile, _ = os.OpenFile(filename, os.O_RDONLY, 0644)
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
	shard := GetShard(key)
	shard.Lock()
	defer shard.Unlock()
	header := make([]byte, 4)
	body := []byte(fmt.Sprintf("set %s %s %d\n", key, value, expiredAt))
	length := uint32(len(body))
	binary.BigEndian.PutUint32(header, length)
	offset, _ := writeFile.Seek(0, io.SeekEnd)
	writeFile.Write(append(header, body...))
	shard.index[key] = Entry{
		Offset:   offset,
		Length:   int64(len(body)),
		ExpireAt: expiredAt,
	}
	lruCache.Put(key, value)
	go Compaction("nosql.json")
	return nil
}

func Get(key string) (string, bool) {
	shard := GetShard(key)

	shard.RLock()
	en, ok := shard.index[key]
	if !ok {
		shard.RUnlock()
		return "", false
	}
	if en.ExpireAt != 0 && time.Now().Unix() > en.ExpireAt {
		shard.RUnlock()
		deleteExpired(key)
		return "", false
	}
	buf := make([]byte, en.Length)
	shard.RUnlock()
	_, err := readFile.ReadAt(buf, en.Offset+4)
	if err != nil && err != io.EOF {
		return "", false
	}
	l := strings.SplitN(string(buf), " ", 5)
	if len(l) < 4 {

		return "", false
	}
	lruCache.Get(key)

	return strings.TrimSpace(l[2]), true
}

func Del(key string) error {
	shard := GetShard(key)

	shard.Lock()
	defer shard.Unlock()
	body := []byte(fmt.Sprintf("del %s\n", key))
	hearder := make([]byte, 4)
	binary.BigEndian.PutUint32(hearder, uint32(len(body)))
	_, err := writeFile.Write(append(hearder, body...))
	if err != nil {
		return err
	}
	delete(shard.index, key)
	lruCache.Remove(key)
	go Compaction("nosql.json")

	return nil
}
func deleteExpired(key string) {
	shard := GetShard(key)

	shard.Lock()
	defer shard.Unlock()
	en, ok := shard.index[key]
	if !ok {
		return
	}
	if en.ExpireAt != 0 && en.ExpireAt < time.Now().Unix() {
		body := []byte(fmt.Sprintf("del %s\n", key))
		hearder := make([]byte, 4)
		binary.BigEndian.PutUint32(hearder, uint32(len(body)))
		_, err := writeFile.Write(append(hearder, body...))
		if err != nil {
			return
		}
		lruCache.Remove(key)
		delete(shard.index, key)
	}
	return
}

func GetMeta(key string) (value string, expiredAt int64, exists bool) {
	shard := GetShard(key)

	shard.RLock()
	defer shard.RUnlock()
	en, ok := shard.index[key]
	if !ok {
		return "", 0, false
	}
	buf := make([]byte, en.Length)
	_, err := readFile.ReadAt(buf, en.Offset+4)
	if err != nil {
		return "", 0, false
	}

	l := strings.SplitN(string(buf), " ", 4)
	if len(l) < 4 {
		return "", 0, false
	}
	if en.ExpireAt != 0 && time.Now().Unix() > en.ExpireAt {
		exists = false
		return "", 0, false
	} else {
		exists = true
	}
	return strings.TrimSpace(l[2]), en.ExpireAt, exists
}

func SetWithExpireAt(key, value string, expiredAt int64) error {
	return setInternal(key, value, expiredAt)
}

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

func HintIsComplete(filename string) bool {
	data, _ := os.ReadFile(filename)
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		if line == "DONE" {
			return true
		}
	}
	return false
}
