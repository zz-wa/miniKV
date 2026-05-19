package store

import (
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
	fi, err := os.Stat(filename)
	if err != nil {
		return false
	}
	if fi.Size() <= 100000 {
		return false
	}

	tmpFile, err := os.OpenFile("nosql.tmp", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return false
	}
	for _, shard := range Shards {
		shard.Lock()

		for key, en := range shard.index {
			buf := make([]byte, lengthPrefixSize+en.Length)
			_, err := readFile.ReadAt(buf, en.Offset)
			if err != nil && err != io.EOF {
				shard.Unlock()
				_ = tmpFile.Close()
				return false
			}
			record, err := decodeRecord(buf)
			if err != nil {
				continue
			}
			if record.Op != OpSet || record.Key != key {
				continue
			}

			newRecord := NewSetRecord(key, record.Value, record.ExpireAt)

			data := encodeRecord(newRecord)

			newOffset, err := tmpFile.Seek(0, io.SeekEnd)
			if err != nil {
				shard.Unlock()
				_ = tmpFile.Close()
				return false
			}
			_, err = tmpFile.Write(data)
			if err != nil {
				continue
			}
			shard.index[key] = Entry{
				Offset:   newOffset,
				Length:   int64(recordBodyLen(newRecord)),
				ExpireAt: en.ExpireAt,
			}

		}
		shard.Unlock()
	}

	err = tmpFile.Close()
	if err != nil {
		return false
	}
	err = os.Rename("nosql.tmp", filename)
	if err != nil {
		return false
	}
	hintFile, err := os.OpenFile("nosql.hint", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return false
	}
	for _, shard := range Shards {
		shard.Lock()
		for key, _ := range shard.index {
			body := fmt.Sprintf("%s %d %d %d\n", key, shard.index[key].Offset, shard.index[key].Length, shard.index[key].ExpireAt)
			_, err2 := hintFile.Write([]byte(body))
			if err2 != nil {
				shard.Unlock()
				return false
			}
		}
		shard.Unlock()
	}

	done := []byte("DONE\n")
	_, err = hintFile.Write(done)
	if err != nil {
		return false
	}

	err = hintFile.Close()
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
	oldWriteFIle := writeFile
	oldReadFile := readFile

	writeFile = newWriteFile
	readFile = newReadFile
	_ = oldWriteFIle.Close()
	_ = oldReadFile.Close()

	return true

}
