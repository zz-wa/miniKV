package store

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Entry struct {
	Offset   int64
	Length   int64
	ExpireAt int64
}

var index = make(map[string]Entry)

var writeFile *os.File
var readFile *os.File
var mu sync.RWMutex

func Set(key, value string, ttl int64) error {
	var expireAt int64
	if ttl > 0 {
		expireAt = time.Now().Unix() + ttl
	}
	return setInternal(key, value, expireAt)
}

func Get(key string) (string, bool) {
	mu.RLock()
	en, ok := index[key]
	if !ok {
		mu.RUnlock()
		return "", false
	}
	if en.ExpireAt != 0 && time.Now().Unix() > en.ExpireAt {
		mu.RUnlock()
		deleteExpired(key)
		return "", false
	}
	buf := make([]byte, en.Length)
	_, err := readFile.ReadAt(buf, en.Offset+4)
	mu.RUnlock()
	if err != nil && err != io.EOF {
		return "", false
	}
	l := strings.SplitN(string(buf), " ", 5)
	if len(l) < 4 {
		return "", false
	}
	return strings.TrimSpace(l[2]), true
}
func deleteExpired(key string) {
	mu.Lock()
	defer mu.Unlock()
	en, ok := index[key]
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

		delete(index, key)
	}
	return
}

func Del(key string) error {
	mu.Lock()
	defer mu.Unlock()
	body := []byte(fmt.Sprintf("del %s\n", key))
	hearder := make([]byte, 4)
	binary.BigEndian.PutUint32(hearder, uint32(len(body)))
	_, err := writeFile.Write(append(hearder, body...))
	if err != nil {
		return err
	}
	delete(index, key)
	Compaction("nosql.json")

	return nil
}

func Open(filename string) error {
	_, err := os.Stat("nosql.tmp")
	if err == nil {
		err = os.Rename("nosql.tmp", filename)
		if err != nil {
			return err
		}
	}
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
				index[l[1]] = Entry{
					Offset:   int64(offset),
					Length:   int64(bodyLen),
					ExpireAt: expireAt,
				}
			}
		}
		if len(l) >= 2 && l[0] == "del" {
			delete(index, strings.TrimSpace(l[1]))
		}
		offset += 4 + bodyLen
	}
	writeFile, _ = os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	readFile, _ = os.OpenFile(filename, os.O_RDONLY, 0644)
	go CleanupExpired()
	return nil
}

func GetMeta(key string) (value string, expiredAt int64, exists bool) {
	mu.RLock()
	defer mu.RUnlock()
	en, ok := index[key]
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

func setInternal(key, value string, expiredAt int64) error {
	mu.Lock()
	defer mu.Unlock()
	header := make([]byte, 4)
	body := []byte(fmt.Sprintf("set %s %s %d\n", key, value, expiredAt))
	length := uint32(len(body))
	binary.BigEndian.PutUint32(header, length)
	offset, _ := writeFile.Seek(0, io.SeekEnd)
	writeFile.Write(append(header, body...))
	index[key] = Entry{
		Offset:   offset,
		Length:   int64(len(body)),
		ExpireAt: expiredAt,
	}
	Compaction("nosql.json")
	return nil
}
