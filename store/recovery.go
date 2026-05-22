package store

import (
	"encoding/binary"
	"os"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
)

func hintIsComplete(filename string) (bool, error) {
	data, err := os.ReadFile(filename)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		if line == "DONE" {
			return true, nil
		}
	}
	return false, nil
}

func recoverPendingCompaction() {
	_, err := os.Stat("nosql.tmp")

	if err == nil {
		err := os.Remove("nosql.tmp")
		if err != nil {
			zap.S().Fatal("remove nosql.tmp fail")
		}
	}
}

func loadIndexFromHint(filename string) error {
	hintData, _ := os.ReadFile(filename)
	for _, line := range strings.Split(string(hintData), "\n") {
		l := strings.SplitN(line, " ", 4)
		if len(l) < 4 {
			continue
		}

		offset, err := strconv.ParseInt(l[1], 10, 64)
		if err != nil {
			return err
		}
		length, err := strconv.ParseInt(l[2], 10, 64)
		if err != nil {
			return err
		}
		expireAt, err := strconv.ParseInt(l[3], 10, 64)
		if err != nil {
			return err
		}
		shard := GetShard(l[0])

		shard.index[l[0]] = Entry{
			Offset:   offset,
			Length:   length,
			ExpireAt: expireAt,
		}
	}
	return nil
}

func rebuildIndexFromLog(filename string) error {
	err := os.Remove("nosql.hint")
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	data, _ := os.ReadFile(filename)
	offset := 0
	for offset < len(data) {
		if offset+lengthPrefixSize > len(data) {
			break
		}
		bodyLen := int(binary.BigEndian.Uint32(data[offset : offset+lengthPrefixSize]))
		end := offset + lengthPrefixSize + bodyLen
		if end > len(data) {
			break
		}

		record, err := decodeRecord(data[offset:end])
		if err != nil {
			offset = end
			continue
		}

		switch record.Op {
		case OpSet:
			if record.ExpireAt == 0 || time.Now().Unix() <= record.ExpireAt {
				shard := GetShard(record.Key)
				shard.index[record.Key] = Entry{
					Offset:   int64(offset),
					Length:   int64(bodyLen),
					ExpireAt: record.ExpireAt,
				}
			}
		case OpDel:
			shard := GetShard(record.Key)
			delete(shard.index, record.Key)
		}
		offset = end
	}
	return nil
}
