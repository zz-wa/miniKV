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
	fi, _ := os.Stat(filename)

	if fi.Size() <= 100000 {
		return false
	}

	tmpFile, _ := os.OpenFile("nosql.tmp", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	for key := range index {
		en := index[key]
		buf := make([]byte, en.Length)
		_, err := readFile.ReadAt(buf, en.Offset+4)
		if err != nil && err != io.EOF {
			tmpFile.Close()
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
		tmpFile.Write(append(header, body...))
		index[key] = Entry{
			Offset:   newOffset,
			Length:   int64(len(body)),
			ExpireAt: en.ExpireAt,
		}
	}
	done := []byte("DONE\n")
	doneHeader := make([]byte, 4)
	binary.BigEndian.PutUint32(doneHeader, uint32(len(done)))

	tmpFile.Write(append(doneHeader, done...))
	writeFile.Close()
	readFile.Close()
	tmpFile.Close()
	os.Rename("nosql.tmp", filename)

	hintFile, _ := os.OpenFile("nosql.hint", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	for key := range index {
		body := fmt.Sprintf("%s %d %d %d\n", key, index[key].Offset, index[key].Length, index[key].ExpireAt)
		hintFile.Write([]byte(body))
	}

	done = []byte("DONE\n")
	hintFile.Write((done))

	hintFile.Close()
	writeFile, _ = os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	readFile, _ = os.OpenFile(filename, os.O_RDONLY, 0644)

	return true

}
