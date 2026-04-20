package store

import (
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
	lines := fmt.Sprintf("set %s %s %d\n", key, value, expireAt)
	mu.Lock()
	defer mu.Unlock()

	offset, _ := writeFile.Seek(0, io.SeekEnd)
	_, err := writeFile.Write([]byte(lines))
	if err != nil {
		return err
	}

	index[key] = Entry{
		Offset:   offset,
		Length:   int64(len(lines)),
		ExpireAt: expireAt,
	}

	Compaction("nosql.json")
	return nil
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
	_, err := readFile.ReadAt(buf, en.Offset)
	mu.RUnlock()
	if err != nil && err != io.EOF {
		return "", false
	}
	l := strings.SplitN(string(buf), " ", 4)
	if len(l) < 3 {
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
		delete(index, key)
		writeFile.Write([]byte("del " + key + " \n"))
	}
	return
}

func Del(key string) error {
	mu.Lock()
	defer mu.Unlock()
	delete(index, key)
	_, err := writeFile.Write([]byte("del " + key + " \n"))
	if err != nil {
		return err
	}
	Compaction("nosql.json")

	return nil
}

func Init(filename string) error {
	_, err := os.Stat("nosql.tmp")
	if err == nil {
		err = os.Rename("nosql.tmp", filename)
		if err != nil {
			return err
		}
	}
	read, err := os.ReadFile(filename)
	lines := strings.Split(string(read), "\n")
	offset := 0
	for _, line := range lines {
		if line == "" {
			offset += 1
			continue
		}
		l := strings.Split(line, " ")
		if l[0] == "set" {
			if len(l) < 4 {
				offset += len(line) + 1
				continue
			}
			expireAt, err := strconv.ParseInt(l[3], 10, 64)
			if err != nil {
				offset += len(line) + 1
				continue
			}
			if expireAt != 0 && time.Now().Unix() > expireAt {
				offset += len(line) + 1
				continue
			}
			index[l[1]] = Entry{
				Offset:   int64(offset),
				Length:   int64(len(line) + 1),
				ExpireAt: expireAt,
			}
		}
		if l[0] == "del" {
			if len(l) < 2 {
				offset += len(line) + 1
				continue
			}
			delete(index, l[1])
		}
		offset += len(line) + 1
	}
	writeFile, _ = os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	readFile, _ = os.OpenFile(filename, os.O_RDONLY, 0644)
	go CleanupExpired()
	return nil
}
