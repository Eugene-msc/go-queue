package queue

import (
	"sync"
    "errors"
)

type Queue[T interface{}] struct {
	head *queueItem[T]
	tail *queueItem[T]

	count        int
	unackedCount int64
	deliveryTag  int

	unacked map[int]*queueItem[T]

	headLock sync.Mutex
	tailLock sync.Mutex
}

const (
	READY   = 0
	UNACKED = 1
)

type queueItem[T interface{}] struct {
	Body *T

	Status int

	Prev *queueItem[T]
	Next *queueItem[T]
}

func (queue *Queue[T]) Init() {
	queue.head = nil
	queue.tail = nil
	queue.count = 0
	queue.unackedCount = 0
	queue.deliveryTag = 0
	queue.unacked = make(map[int]*queueItem[T])
}

// Push a message to internalQueue
func (q *Queue[T]) Push(m *T) {
	item := queueItem[T]{Body: m}

	q.tailLock.Lock()
	var empty = false
	if q.head == nil {
		empty = true
		q.headLock.Lock()
	}

	if q.head == nil {
		q.head = &item
	}
	if q.tail == nil {
		q.tail = &item
	} else {
		q.tail.Next = &item

		item.Prev = q.tail
		q.tail = &item
	}

	q.count++

	if empty {
		q.headLock.Unlock()
	}
	q.tailLock.Unlock()
}

// Get a message from Queue (needs to be Acked after)
func (q *Queue[T]) Get() (*T, int, error) {
	var m *T
	if q.head == nil {
		return m, 0, ErrEmptyQueue
	}

	q.headLock.Lock()
	var im = q.head
	for {
		if im.Status == READY {
			im.Status = UNACKED
			break
		}
		if im.Next == nil {
			q.headLock.Unlock()
			return m, 0, ErrEmptyQueue
		}

		im = im.Next
	}
	q.deliveryTag = (q.deliveryTag + 1) % 100000
	q.unacked[q.deliveryTag] = im
	q.unackedCount++
	m = im.Body

	q.headLock.Unlock()

	return m, q.deliveryTag, nil
}

// Ack a message previously retrieved with Get by its deliveryTag
func (q *Queue[T]) Ack(deliveryTag int) bool {
	q.headLock.Lock()
	im, exists := q.unacked[deliveryTag]
	if !exists {
		q.headLock.Unlock()
		return true
	}

	if im.Prev == nil {
		q.head = im.Next
	} else {
		im.Prev.Next = im.Next
	}
	if im.Next == nil {
		q.tailLock.Lock()
		q.tail = im.Prev
		q.tailLock.Unlock()
	} else {
		im.Next.Prev = im.Prev
	}

	q.count -= 1
	q.unackedCount--
	delete(q.unacked, deliveryTag)
	q.headLock.Unlock()

	return true
}

// Nack a message previously retrieved with Get by its deliveryTag
func (q *Queue[T]) Nack(deliveryTag int) bool {
	q.headLock.Lock()
	im, exists := q.unacked[deliveryTag]
	if !exists {
		q.headLock.Unlock()
		return true
	}
	im.Status = READY
	q.unackedCount--
	delete(q.unacked, deliveryTag)
	q.headLock.Unlock()

	return true
}

// Length of the internal queue
func (q *Queue[T]) Length() int {
	return q.count
}

// ErrNoQueue error queue does not exist
var ErrNoQueue = errors.New("no queue")

// ErrEmptyQueue error queue is empty
var ErrEmptyQueue = errors.New("empty queue")
