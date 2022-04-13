package kv

import (
	"fmt"
)

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

func (c *ClockPolicy) Pin(id NodeID) bool {
	return c.clock.Remove(id)
}

func (c *ClockPolicy) Unpin(id NodeID) {
	if !c.clock.HasKey(id) {
		c.clock.Insert(id, true)
	}
}

// Victim removes the victim frame as defined by the replacement policy
func (c *ClockPolicy) Victim() (NodeID, error) {

	if c.clock.IsEmpty() {
		return 0, fmt.Errorf("no node to evict, all nodes are in use")
	}

	var victimFrameID NodeID

	for {
		currentNode := c.clock.Next()

		if currentNode.value.(bool) {
			currentNode.value = false
		} else {
			key, _ := currentNode.key.(NodeID)
			frameID := key
			victimFrameID = frameID

			c.clock.Remove(currentNode.key)

			return victimFrameID, nil
		}

	}
}
