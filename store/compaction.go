package store

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strings"
)

func Compaction(filename string) bool {
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
	writeFile.Close()
	readFile.Close()
	tmpFile.Close()
	os.Remove(filename)
	os.Rename("nosql.tmp", filename)
	writeFile, _ = os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	readFile, _ = os.OpenFile(filename, os.O_RDONLY, 0644)

	return true

}
