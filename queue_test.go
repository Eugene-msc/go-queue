package go_queue

import (
	"strconv"
	"testing"
	"time"
)

func TestBasicPushGet(t *testing.T) {
    q := new(Queue[int])
    q.Init()

    if (q.Length() != 0) {
        t.Log("Queue length should be 0 after Init() - got " + strconv.Itoa(q.Length()))
        t.Fail()
    }

    item := 123
    q.Push(&item)
    if (q.Length() != 1) {
        t.Log("Queue length should be 1 after Push() - got " + strconv.Itoa(q.Length()))
        t.Fail()
    }

    got, deliveryTag, _ := q.Get()

    if (*got != 123) {
        t.Log("Should have received 123 back - got " + strconv.Itoa(*got))
        t.Fail()
    }

    q.Ack(deliveryTag)
    
    if (q.Length() != 0) {
        t.Log("Queue length should be 0 after Ack() - got " + strconv.Itoa(q.Length()))
        t.Fail()
    }
}

func TestMultiPush(t *testing.T) {
    q := new(Queue[int])
    q.Init()

    if (q.Length() != 0) {
        t.Log("Queue length should be 0 after Init() - got " + strconv.Itoa(q.Length()))
        t.Fail()
    }

    item1, item2, item3 := 1, 2, 3
    q.Push(&item1)
    q.Push(&item2)
    q.Push(&item3)
    if (q.Length() != 3) {
        t.Log("Queue length should be 3 after Push() - got " + strconv.Itoa(q.Length()))
        t.Fail()
    }

    got1, deliveryTag1, _ := q.Get()
    got2, deliveryTag2, _ := q.Get()
    got3, deliveryTag3, _ := q.Get()

    if (*got1 != 1) {
        t.Log("Should have received 1 back - got " + strconv.Itoa(*got1))
        t.Fail()
    }
    if (*got2 != 2) {
        t.Log("Should have received 2 back - got " + strconv.Itoa(*got2))
        t.Fail()
    }
    if (*got3 != 3) {
        t.Log("Should have received 3 back - got " + strconv.Itoa(*got3))
        t.Fail()
    }

    q.Ack(deliveryTag1)
    q.Ack(deliveryTag2)
    q.Ack(deliveryTag3)
    
    if (q.Length() != 0) {
        t.Log("Queue length should be 0 after Ack() - got " + strconv.Itoa(q.Length()))
        t.Fail()
    }
}

func TestRace(t *testing.T) {
    q := new(Queue[int])
    q.Init()
    const LIMIT = 1000000

    slice := [LIMIT]int{}
    for i := 0; i < LIMIT; i++ {
        slice[i] = i
    }

    for i := 0; i < LIMIT; i++ {
        go q.Push(&slice[i])
    }

    time.Sleep(100 * time.Millisecond)

    if q.Length() != LIMIT {
        t.Log("Queue length should be "+strconv.Itoa(LIMIT)+" after Pushes - got " + strconv.Itoa(q.Length()))
        t.Fail()
    }

    i := 0
    for ;q.Length() > 0; {
        _, deliveryTag, _ := q.Get()
        q.Ack(deliveryTag)
        i ++;

        if i > LIMIT {
            t.Log("i cannot get bigger than "+strconv.Itoa(LIMIT)+" - got " + strconv.Itoa(i))
            t.Fail()
        }
    }

    if i != LIMIT {
        t.Log("i should be "+strconv.Itoa(LIMIT)+" after emptying the queue " + strconv.Itoa(i))
        t.Fail()
    }

}
