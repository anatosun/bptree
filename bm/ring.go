package bm

import (
    "errors"
    "fmt"
    "math"
)

type Ring struct {
    capacity int
    size     int
    pointer  int
    elements []RingNode
}

type RingNode struct {
    key   interface{}
    value interface{}
}

func NewRing(capacity int) *Ring {
    elements := make([]RingNode, capacity)
    return &Ring{capacity: capacity, size: 0, pointer: 0, elements: elements}
}

func (ring *Ring) Insert(key interface{}, val interface{}) error {
    if ring.size < ring.capacity { // size \in [0...capacity-1]; capacity e.g. 5 => size \in 0...4
        ring.elements[ring.size] = RingNode{key, val}
        ring.size++
        return nil
    } else {
        return errors.New("ring.go: Ring capacity has been reached, cannot insert.")
    }
}

func (ring *Ring) Update(key interface{}, newval interface{}) bool {
    i := ring.IndexOf(key)
    if i == -1 {
        return false
    } else {
        (&ring.elements[i]).value = newval
        return true
    }
}

func (ring *Ring) Next() *RingNode {

    ring.pointer = safePointerComputation(ring.pointer, ring.size)
    el := &(ring.elements[ring.pointer])
    //fmt.Printf("address1=%p\n", el)
    ring.pointer++
    return el
}

func (ring *Ring) Prev() RingNode {
    if ring.pointer == 0 {
        ring.pointer = ring.size - 1 // remember, size starts at 1, pointer at 0

    } else {
        if ring.pointer > 0 {
            ring.pointer--
        } else {
            panic(fmt.Errorf("ring.go: This shouldn't happen, abort..."))
        }
    }

    el := ring.elements[ring.pointer]

    return el
}

func (ring *Ring) Find(key interface{}) *RingNode {
    for i := 0; i < ring.size; i++ {
        nextNode := ring.elements[i]
        if key == nextNode.key {
            return &nextNode
        }
    }
    return nil
}

func (ring *Ring) IndexOf(key interface{}) int {
    for i := 0; i < ring.size; i++ {
        nextNode := ring.elements[i]
        if key == nextNode.key {
            return i
        }
    }
    return -1
}

func (ring *Ring) Remove(key interface{}) bool {
    i := ring.IndexOf(key)
    if i == -1 {
        return false
    } else {
        ring.elements = append(ring.elements[:i], ring.elements[i+1:]...) // not so efficient
        ring.elements = append(ring.elements, RingNode{})

        if ring.pointer == ring.size || ring.pointer == i && i != 0 {
            //move down
            ring.pointer--
        } else if i == 0 {
            ring.pointer = ring.size - 1
        }

        ring.size--

        return true
    }
}

func (ring *Ring) HasKey(node interface{}) bool {
    return ring.Find(node) != nil
}

func safePointerComputation(pointer int, size int) int {
    return int(math.Abs(float64(pointer % size)))
}

func (ring *Ring) Print() {
    for i := 0; i < ring.size; i++ {
        fmt.Printf("[%d]: %s\n", i, ring.elements[i])
    }
}

func (ring *Ring) IsFull() bool {
    return ring.size == ring.capacity
}
func (ring *Ring) IsEmpty() bool {
    return ring.size == 0
}
