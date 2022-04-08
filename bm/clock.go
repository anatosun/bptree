package bm

import (
	"fmt"
)

const debug bool = false

type ClockPolicy struct {
	clock *Ring
}

func NewClockPolicy(capacity int) *ClockPolicy {
	ring := NewRing(capacity)
	return &ClockPolicy{ring}
}

func (c *ClockPolicy) Size() int {
	return c.clock.size
}

func (c *ClockPolicy) Pin(id PageID) bool {
	return c.clock.Remove(id)
}

func (c *ClockPolicy) Unpin(id PageID) {
	if !c.clock.HasKey(id) {
		c.clock.Insert(id, true)
	}
}

// Victim removes the victim frame as defined by the replacement policy
func (c *ClockPolicy) Victim() *PageID {

	if c.clock.IsEmpty() {
		return nil
	}

	var victimFrameID *PageID

	if debug {
		fmt.Println("Running victim....")
	}

	for {
		currentNode := c.clock.Next()

		if debug {
			fmt.Printf("node: key=%d ; value=%t \n", currentNode.key.(int), currentNode.value.(bool))
		}

		if currentNode.value.(bool) {
			currentNode.value = false
		} else {
			key, _ := currentNode.key.(PageID)
			frameID := key
			victimFrameID = &frameID

			if debug {
				fmt.Printf("returning node: key=%d ; value=%t ; pointer of ring=%d\n", currentNode.key.(int), currentNode.value.(bool), c.clock.pointer)
			}

			c.clock.Remove(currentNode.key)

			if debug {
				fmt.Printf("pointer of ring after removal=%d\n", c.clock.pointer)
			}

			return victimFrameID
		}

	}

	if debug {
		fmt.Println("Nothing returned... ending victim")
	}
	return nil
}
