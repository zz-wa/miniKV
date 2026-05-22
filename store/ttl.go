package store

import (
	"time"
)

func CleanupExpired(cfg Config, stopCh <-chan struct{}) {
	ticker := time.NewTicker(cfg.TTLScanInternal)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			for _, shards := range Shards {
				cleanupShard(shards)
			}
		case <-stopCh:
			return
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
