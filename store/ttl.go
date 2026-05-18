package store

import "time"

func CleanupExpired() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		for _, shards := range Shards {
			cleanupShard(shards)
		}
	}
}

func cleanupShard(s *Shard) {
	fileMu.Lock()
	defer fileMu.Unlock()
	s.Lock()
	defer s.Unlock()
	for key, en := range s.index {
		if en.ExpireAt != 0 && en.ExpireAt <= time.Now().Unix() {
			err := delLocked(s, key)
			if err != nil {
				continue
			}

		}
	}
}
