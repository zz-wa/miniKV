package store

import (
	"io"
	"time"
)

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

func SetWithExpireAt(key, value string, expiredAt int64) error {
	return setInternal(key, value, expiredAt)
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
		shard.RUnlock()
		fileMu.RUnlock()
		deleteExpired(key)
		return "", false
	}

	if value, ok := lruCache.Get(key); ok {
		shard.RUnlock()
		fileMu.RUnlock()
		return value, true
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

	lruCache.Put(key, record.Value)
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
