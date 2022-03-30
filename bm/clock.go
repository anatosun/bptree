package bm

import (
//	"fmt"
)

type ClockPolicy struct {
	clock *Ring
	hand  int
}

func NewClockPolicy(capacity int) *ClockPolicy {
	var ring *Ring
	ring = NewRing(capacity)
	return &ClockPolicy{ring, 0}
}

func (c *ClockPolicy) Size() int {
	return c.clock.size
}

func (c *ClockPolicy) Pin(id int) bool {
	return c.clock.Remove(id)
}

func (c *ClockPolicy) Unpin(id int) {
	if !c.clock.HasKey(id) {
		c.clock.Insert(id, true)
		// if c.cList.size == 1 {
		// 	c.clockHand = &c.cList.head
		// }
	}
}

// Victim removes the victim frame as defined by the replacement policy
func (c *ClockPolicy) Victim() *int {

	if c.clock.IsEmpty() {
		return nil
	}

	var victimFrameID *int

	for {
		currentNode := c.clock.Next()

		if currentNode.value.(bool) {
			currentNode.value = false

			//key, _ := currentNode.key.(int)
			//c.hand = key

		} else {
			key, _ := currentNode.key.(int)
			frameID := key
			victimFrameID = &frameID

			//c.hand, _ = c.clock.Next().key.(int)

			c.clock.Remove(currentNode.key)
			return victimFrameID
		}

	}

	return nil
}
