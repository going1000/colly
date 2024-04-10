package main

import (
	"fmt"
	"github.com/gocolly/colly/v2"
	queue "github.com/gocolly/colly/v2/gQueue"
	"runtime"
)

func main() {
	fmt.Println("goroutine queue", runtime.NumGoroutine())
	for i := 0; i < 3; i++ {
		test()
		fmt.Println("goroutine queue", runtime.NumGoroutine())
	}
	fmt.Println("goroutine queue", runtime.NumGoroutine())
}

func test() {

	url := "https://httpbin.org/delay/1"

	// Instantiate default collector
	c := colly.NewCollector(colly.AllowURLRevisit())

	// create a request queue with 2 consumer threads
	q, _ := queue.New(
		5, // Number of consumer threads
		&queue.InMemoryQueueStorage{MaxSize: 10000}, // Use default queue storage
	)

	c.OnRequest(func(r *colly.Request) {
		fmt.Println("visiting", r.URL)
		if r.ID < 15 {
			//fmt.Println("r.ID done ", r.ID)
			r2, err := r.New("GET", fmt.Sprintf("%s?x=%v", url, r.ID), nil)
			if err == nil {
				q.AddRequest(r2)
			}
		} else {
			q.Stop()
		}
	})

	for i := 0; i < 10; i++ {
		// Add URLs to the queue
		q.AddURL(fmt.Sprintf("%s?n=%d", url, i))
	}
	// Consume URLs
	q.Run(c)
}
