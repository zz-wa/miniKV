package store

import (
	"hash/fnv"
	"sync"
)

type Entry struct {
	Offset   int64
	Length   int64
	ExpireAt int64
}

type Shard struct {
	index map[string]Entry
	sync.RWMutex
}

func NewShard() *Shard {
	s := &Shard{
		index: make(map[string]Entry),
	}
	return s
}

func GetShard(key string) *Shard {
	i := hash(key)
	return Shards[i%16]
}

func hash(key string) uint32 {
	hasher := fnv.New32()
	_, err := hasher.Write([]byte(key))
	if err != nil {
		return 0
	}
	return hasher.Sum32()
}
