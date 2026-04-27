package store

import "time"

func CleanupExpired() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		for _, shrads := range Shards {
			shrads.Lock()
			for key, en := range shrads.index {
				if en.ExpireAt != 0 && en.ExpireAt <= time.Now().Unix() {
					delete(shrads.index, key)
				}
			}
			shrads.Unlock()
		}
	}
}
