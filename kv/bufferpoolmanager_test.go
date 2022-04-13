package kv

import (
	"fmt"
	"testing"
)

func TestBufferPoolManager(t *testing.T) {

	clock := NewClockPolicy(5)
	disk := NewDiskManager()
	bpm := NewBufferPoolManager(disk, clock)

	const n int = 20 //asume n<=Bufferpooolcapacity, don't change this since the tests are bound to this! (order, size..)

	// Get 20 new nodes
	for i := 0; i < n; i++ {
		bpm.GetNewNode() //id=i+1
	}

	AssertEqual(t, len(bpm.pool), BufferPoolCapacity)

	// Unpin node with id=2 and set dirty bit
	testingNode, _ := bpm.FetchNode(2)
	AssertEqual(t, testingNode.getPinCounter(), uint64(1))
	AssertEqual(t, bpm.UnpinNode(testingNode.getID(), true), nil) //if not nill => error
	AssertEqual(t, testingNode.getPinCounter(), uint64(0))
	AssertEqual(t, testingNode.IsDirty(), true)

	// Nodes currently in Clock, should be 5 (full)
	// Very specific to the clock, but to test whether bufferpool
	// actually adds them to the clock. Must be replaced for other replace policy
	// Also, let's define a lambda expr here
	getClockSize := func() int { return (*bpm.replacePolicy.clock).GetSize() }
	AssertEqual(t, getClockSize(), 5)

	// Buffer is full, clock has multiple unpinned nodes (pincount=0), try to get a new node
	testingNodeID, _ := bpm.GetNewNode() //id=21
	testingNode, _ = bpm.FetchNode(testingNodeID)
	AssertEqual(t, testingNode.getID(), NodeID(21))

	//Size of clock again, now 4 (one should have been removed from Clock, i.e. page evicted)
	AssertEqual(t, getClockSize(), 4)

	//Unpin
	bpm.UnpinNode(testingNode.getID(), false)

	// Node 200 doesn't exist, shouldn't return nil but an error instead
	AssertNotEqual(t, bpm.UnpinNode(200, false), nil)

	//Fill up the buffer pool
	for i := 0; i < BufferPoolCapacity; i++ {
		_, err := bpm.FetchNode(NodeID(i + 1))
		if err != nil {
			fmt.Println(err)
		}
	}
	// Pool is full now, also all nodes are in use, so nothing to evict, should return error
	_, err := bpm.GetNewNode()
	AssertNotEqual(t, err, nil)

	//Make room in the bufferpool by setting some counters back to 0 (3/5 will be 0 after this and thus added to the clock)
	for i := 0; i < BufferPoolCapacity-2; i++ {
		err := bpm.UnpinNode(NodeID(i+1), true)
		if err != nil {
			fmt.Println(err)
		}
	}
	AssertEqual(t, getClockSize(), 3)

	// Put node(id=11, still in buffer pool) into the clock
	testingNodeID = NodeID(11)
	_, err = bpm.FetchNode(testingNodeID)
	if err != nil {
		fmt.Println(err)
	}

	AssertEqual(t, getClockSize(), 2)
	err = bpm.UnpinNode(testingNodeID, false) //add it to the clock
	if err != nil {
		fmt.Println(err)
	}
	AssertEqual(t, getClockSize(), 3)

	// Now fetch it (from buffer pool) (and hence remove it from the clock again)
	bpm.FetchNode(testingNodeID)
	bpm.FetchNode(testingNodeID)
	bpm.FetchNode(testingNodeID)
	testingNode, _ = bpm.FetchNode(testingNodeID)

	// Counter should be at 4 now
	AssertEqual(t, testingNode.getPinCounter(), uint64(4))
	testingNode.setPinCounter(1) //back to 1, fast hack to not unpin x times. never use this!
	bpm.UnpinNode(testingNodeID, false)

	// Unpin all nodes with id  5,4 (Currently in bufferpool with counter 1)
	AssertEqual(t, getClockSize(), 3)
	AssertEqual(t, bpm.UnpinNode(NodeID(5), false), nil)
	AssertEqual(t, bpm.UnpinNode(NodeID(4), false), nil)
	AssertEqual(t, getClockSize(), 5)
}
