package store

import (
	"container/list"
)

type LRUCache struct {
	Capacity int
	cache    map[string]*list.Element
	list     *list.List
}

type Pair struct {
	key   string
	value string
}

func NewLRUCache(capacity int) *LRUCache {
	return &LRUCache{
		Capacity: capacity,
		cache:    make(map[string]*list.Element),
		list:     list.New(),
	}
}

func (c *LRUCache) Get(key string) string {
	if elem, ok := c.cache[key]; ok {
		c.list.MoveToFront(elem)
		return elem.Value.(*Pair).value
	}
	return ""
}

func (l *LRUCache) Put(key string, value string) {
	if elem, ok := l.cache[key]; ok {
		elem.Value.(*Pair).value = value
		l.list.MoveToFront(elem)
		return
	}
	if l.list.Len() >= l.Capacity {
		lru := l.list.Back()
		if lru != nil {

			delete(l.cache, lru.Value.(*Pair).key)
			l.list.Remove(lru)

			delete(GetShard(key).index, lru.Value.(*Pair).key)
		}
	}
	pair := &Pair{key, value}
	elem := l.list.PushFront(pair)
	l.cache[key] = elem
}

func (l *LRUCache) Remove(key string) {
	if elem, ok := l.cache[key]; ok {
		l.list.Remove(elem)
		delete(l.cache, key)
	}
}
