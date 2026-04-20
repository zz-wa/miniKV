package store

import (
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
		_, err := readFile.ReadAt(buf, en.Offset)
		if err != nil && err != io.EOF {
			tmpFile.Close()
			return false
		}
		l := strings.SplitN(string(buf), " ", 4)
		if len(l) < 4 {
			continue
		}

		value := l[2]

		newLine := fmt.Sprintf("set %s %s %d\n", key, value, en.ExpireAt)
		newOffset, _ := tmpFile.Seek(0, io.SeekEnd)
		index[key] = Entry{
			Offset:   newOffset,
			Length:   int64(len(newLine)),
			ExpireAt: en.ExpireAt,
		}
		tmpFile.Write([]byte(newLine))
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
