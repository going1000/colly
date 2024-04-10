package gQueue

import (
	"net/url"
	"sync"

	"github.com/gocolly/colly/v2"
)

// Queue is a request queue which uses a Collector to consume
// requests in multiple threads
type Queue struct {
	// Threads defines the number of consumer threads
	Threads int
	storage Storage
	mut     sync.Mutex // guards running
	running bool
}

// New creates a new queue with a Storage specified in argument
// A standard InMemoryQueueStorage is used if Storage argument is nil.
func New(threads int, s Storage) (*Queue, error) {
	if s == nil {
		s = &InMemoryQueueStorage{MaxSize: 100000}
	}
	if err := s.Init(); err != nil {
		return nil, err
	}
	return &Queue{
		Threads: threads,
		storage: s,
		running: false,
	}, nil
}

// IsEmpty returns true if the queue is empty
func (q *Queue) IsEmpty() bool {
	s, _ := q.Size()
	return s == 0
}

// AddURL adds a new URL to the queue
func (q *Queue) AddURL(URL string) error {
	u, err := urlParser.Parse(URL)
	if err != nil {
		return err
	}
	u2, err := url.Parse(u.Href(false))
	if err != nil {
		return err
	}
	r := &colly.Request{
		URL:    u2,
		Method: "GET",
	}
	d, err := r.Marshal()
	if err != nil {
		return err
	}
	return q.storage.AddRequest(d)
}

// AddRequest adds a new Request to the queue
func (q *Queue) AddRequest(r *colly.Request) error {
	return q.storeRequest(r)
}

func (q *Queue) storeRequest(r *colly.Request) error {
	d, err := r.Marshal()
	if err != nil {
		return err
	}
	return q.storage.AddRequest(d)
}

// Size returns the size of the queue
func (q *Queue) Size() (int, error) {
	return q.storage.QueueSize()
}

// Run starts consumer threads and calls the Collector
// to perform requests. Run blocks while the queue has active requests
// The given Storage must not be used directly while Run blocks.
func (q *Queue) Run(c *colly.Collector) error {
	q.mut.Lock()
	if q.running == true {
		q.mut.Unlock()
		panic("cannot call duplicate Queue.Run")
	}
	q.running = true
	q.mut.Unlock()

	var wg sync.WaitGroup

	// 设置并发数
	limiter := make(chan bool, q.Threads)

	for {
		size, err := q.storage.QueueSize()
		if err != nil {
			// TODO 中断恢复
			return err
		}

		if size > 0 && q.running {
			wg.Add(1)
			limiter <- true
			go func(limiter chan bool, wg *sync.WaitGroup) {
				defer wg.Done()

				req, err := q.loadRequest(c)
				if err != nil {
					// ignore an error returned by GetRequest() or
					// UnmarshalRequest()
				} else {
					req.Do()
				}

				<-limiter
			}(limiter, &wg)
		} else {
			break
		}
	}

	wg.Wait()
	return nil
}

// Stop will stop the running queue
func (q *Queue) Stop() {
	q.mut.Lock()
	q.running = false
	q.mut.Unlock()
}

func (q *Queue) loadRequest(c *colly.Collector) (*colly.Request, error) {
	buf, err := q.storage.GetRequest()
	if err != nil {
		return nil, err
	}
	copied := make([]byte, len(buf))
	copy(copied, buf)
	return c.UnmarshalRequest(copied)
}
