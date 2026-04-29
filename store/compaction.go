package store

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strings"
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
			buf := make([]byte, en.Length)
			_, err := readFile.ReadAt(buf, en.Offset+4)
			if err != nil && err != io.EOF {
				shard.Unlock()
				err := tmpFile.Close()
				if err != nil {
					return false
				}
				return false
			}
			l := strings.SplitN(string(buf), " ", 4)
			if len(l) < 3 {
				continue
			}
			body := []byte(fmt.Sprintf("set %s %s %d\n", key, l[2], en.ExpireAt))
			header := make([]byte, 4)
			binary.BigEndian.PutUint32(header, uint32(len(body)))
			newOffset, _ := tmpFile.Seek(0, io.SeekEnd)
			_, err = tmpFile.Write(append(header, body...))
			if err != nil {
				shard.Unlock()
				return false
			}
			shard.index[key] = Entry{
				Offset:   newOffset,
				Length:   int64(len(body)),
				ExpireAt: en.ExpireAt,
			}
		}
		shard.Unlock()
	}

	done := []byte("DONE\n")
	doneHeader := make([]byte, 4)
	binary.BigEndian.PutUint32(doneHeader, uint32(len(done)))

	_, err = tmpFile.Write(append(doneHeader, done...))
	if err != nil {
		return false
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

	done = []byte("DONE\n")
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
