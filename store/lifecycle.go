package store

import (
	"errors"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
)

var Shards [16]*Shard
var stopCh chan struct{}
var isCompaction atomic.Bool
var lruCache *LRUCache

var writeFile *os.File
var readFile *os.File
var fileMu sync.RWMutex
var ttlWg sync.WaitGroup

func Open(cfg Config) error {
	currentConfig = cfg

	for i := 0; i < 16; i++ {
		Shards[i] = NewShard()
	}
	recoverPendingCompaction(cfg.TmpFile)

	lruCache = NewLRUCache(cfg.LRUSize)

	ok, err := hintIsComplete(cfg.HintFile)
	if err != nil {
		return err
	}
	switch ok {
	case true:
		err := loadIndexFromHint(cfg)
		if err != nil {
			return err
		}
	case false:
		err := rebuildIndexFromLog(cfg)
		if err != nil {
			return err
		}
	}

	wf, err := os.OpenFile(cfg.DataFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	rf, err := os.OpenFile(cfg.DataFile, os.O_RDONLY, 0644)
	if err != nil {
		closeErr := wf.Close()
		if closeErr != nil {
			return errors.Join(err, closeErr)
		}
		return err
	}

	writeFile = wf
	readFile = rf
	stopCh = make(chan struct{})
	ttlWg.Add(1)
	go func() {
		defer ttlWg.Done()
		CleanupExpired(cfg, stopCh)
	}()

	return nil

}

func Close() error {
	close(stopCh)
	ttlWg.Wait()

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
