package store

import (
	"errors"
	"fmt"
	"io"
	"os"
)

func Compaction(filename string) bool {

	if !isCompaction.CompareAndSwap(false, true) {
		return false
	}
	defer isCompaction.Store(false)
	fileMu.Lock()
	defer fileMu.Unlock()
	if !shouldCompact(filename) {
		return false
	}

	newIndex, err := buildCompactedFile("nosql.tmp")
	if err != nil {
		return false
	}
	err = os.Remove("nosql.hint")
	if err != nil && !os.IsNotExist(err) {
		return false
	}

	err = os.Rename("nosql.tmp", filename)
	if err != nil {
		return false
	}
	newWriteFile, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return false
	}
	newReadFile, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		_ = newWriteFile.Close()
		return false
	}

	oldWriteFile := writeFile
	oldReadFile := readFile

	writeFile = newWriteFile
	readFile = newReadFile
	_ = oldWriteFile.Close()
	_ = oldReadFile.Close()

	for i, shard := range Shards {
		shard.Lock()
		shard.index = newIndex[i]
		shard.Unlock()
	}
	err = writeHintFromIndex("nosql.hint", newIndex)
	if err != nil {
		return true
	}

	return true

}

func shouldCompact(filename string) bool {
	fi, err := os.Stat(filename)
	if err != nil {
		return false
	}
	if fi.Size() <= 100000 {
		return false
	}
	return true
}

func buildCompactedFile(tmpName string) ([]map[string]Entry, error) {
	tmpFile, err := os.OpenFile(tmpName, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return nil, err
	}

	newIndex := make([]map[string]Entry, len(Shards))
	for i := range newIndex {
		newIndex[i] = make(map[string]Entry)
	}

	for i, shard := range Shards {
		shard.RLock()

		for key, en := range shard.index {
			buf := make([]byte, lengthPrefixSize+en.Length)
			_, err := readFile.ReadAt(buf, en.Offset)
			if err != nil && err != io.EOF {
				shard.RUnlock()
				_ = tmpFile.Close()
				return nil, err
			}
			record, err := decodeRecord(buf)
			if err != nil {
				shard.RUnlock()
				_ = tmpFile.Close()
				return nil, errors.New("decode failed")
			}
			if record.Op != OpSet || record.Key != key {
				shard.RUnlock()
				_ = tmpFile.Close()
				return nil, errors.New("record  mismatch")
			}

			newRecord := NewSetRecord(key, record.Value, record.ExpireAt)

			data := encodeRecord(newRecord)

			newOffset, err := tmpFile.Seek(0, io.SeekEnd)
			if err != nil {
				shard.RUnlock()
				_ = tmpFile.Close()
				return nil, err

			}
			_, err = tmpFile.Write(data)
			if err != nil {
				shard.RUnlock()
				_ = tmpFile.Close()
				return nil, err
			}
			newIndex[i][key] = Entry{
				Offset:   newOffset,
				Length:   int64(recordBodyLen(newRecord)),
				ExpireAt: en.ExpireAt,
			}

		}
		shard.RUnlock()
	}
	err = tmpFile.Close()
	if err != nil {
		return nil, err
	}

	return newIndex, nil
}

func writeHintFromIndex(filename string, newIndex []map[string]Entry) error {
	hintFile, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		_ = os.Remove(filename)
		return err
	}
	for i := range newIndex {
		for key, en := range newIndex[i] {
			body := fmt.Sprintf("%s %d %d %d\n", key, en.Offset, en.Length, en.ExpireAt)
			_, err2 := hintFile.Write([]byte(body))
			if err2 != nil {
				_ = hintFile.Close()
				_ = os.Remove(filename)
				return err2
			}

		}

	}

	done := []byte("DONE\n")
	_, err = hintFile.Write(done)
	if err != nil {
		_ = hintFile.Close()
		_ = os.Remove(filename)
		return err
	}

	err = hintFile.Close()
	if err != nil {
		_ = os.Remove(filename)
		return err
	}
	return nil
}
