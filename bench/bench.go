package main

import (
	"bufio"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	start := time.Now()
	var total int64
	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			conn, err := net.Dial("tcp", "localhost:8080")
			if err != nil {
				return
			}

			reader := bufio.NewReader(conn)

			for j := 0; j < 100; j++ {
				conn.Write([]byte(fmt.Sprintf("set key%d value\n", j)))
				reader.ReadString('\n')
				atomic.AddInt64(&total, 1)
			}

		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	fmt.Printf("  SET QPS:%.0f\n", float64(total)/elapsed.Seconds())
	total = 0
	start = time.Now()
	var wg2 sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg2.Add(1)
		go func() {
			defer wg2.Done()
			conn, err := net.Dial("tcp", "localhost:8080")
			if err != nil {
				return
			}
			reader := bufio.NewReader(conn)

			for j := 0; j < 1000; j++ {
				conn.Write([]byte(fmt.Sprintf("get key%d\n", j%100)))
				reader.ReadString('\n')
				atomic.AddInt64(&total, 1)
			}
		}()
	}
	wg2.Wait()
	elapsed = time.Since(start)
	fmt.Printf("GET  QPS :%0.f\n", float64(total)/elapsed.Seconds())
}
