package server

import (
	"errors"
	"kv/store"
)

type Txn struct {
	active  bool //事务是否在进行中
	undoLog map[string]undoEntry
}

type undoEntry struct {
	existed  bool
	value    string
	expireAt int64
}

func (t *Txn) Begin() error {
	if t.active {
		return errors.New("transaction already active")
	}
	t.active = true
	t.undoLog = make(map[string]undoEntry)
	return nil
}

func (t *Txn) Commit() error {
	if !t.active {
		return errors.New("no active transaction")
	}
	t.active = false
	t.undoLog = nil
	return nil
}

func (t *Txn) Rollback() error {
	if !t.active {
		return errors.New("no active transaction")
	}
	var firstErr error

	for key, en := range t.undoLog {
		if en.existed {
			if err := store.SetWithExpireAt(key, en.value, en.expireAt); err != nil && firstErr == nil {
				firstErr = err
			}
		} else {
			if err := store.Del(key); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	t.active = false
	t.undoLog = nil
	return firstErr
}

func (t *Txn) RecordFirst(key string) {
	if _, ok := t.undoLog[key]; ok {
		return
	}
	value, expireAt, exists := store.GetMeta(key)
	t.undoLog[key] = undoEntry{
		value:    value,
		expireAt: expireAt,
		existed:  exists,
	}
}
func (t *Txn) IsActive() bool {
	return t.active
}
