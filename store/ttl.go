package store

import "time"

func CleanupExpired() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		mu.Lock()
		for key, en := range index {
			if en.ExpireAt != 0 && en.ExpireAt <= time.Now().Unix() {
				delete(index, key)
				writeFile.Write([]byte("del " + key + " \n"))
			}
		}
		mu.Unlock()
	}
}
