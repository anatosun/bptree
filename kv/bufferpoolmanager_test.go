package kv

import (
	"testing"
)

func TestBufferPoolManager(t *testing.T) {

	clock := NewClockPolicy(BufferPoolCapacity)
	disk := NewDiskManagerMock()
	bpm := NewBufferPoolManager(disk, clock)

	const n int = 20//asume n<=Bufferpooolcapacity, don't change this since the tests are bound to this! (order, sitze..)

	// Get 4 new nodes
	for i := 0; i < n; i++ {
		bpm.GetNewNode() //id=i
	}

	AssertEqual(t, len(bpm.pool), BufferPoolCapacity)

	// Unpin node with id=2 and set dirty bit
	AssertEqual(t, (*bpm.pool[2]).getPinCounter(), uint64(1))
	AssertEqual(t, bpm.UnpinNode(2, true), nil) //if not nill => error
	AssertEqual(t, (*bpm.pool[2]).getPinCounter(), uint64(0))
	AssertEqual(t, (*bpm.pool[2]).IsDirty(), true)

	// Nodes currently in Clock, should be 1
	// Very specific to the clock, but to test whether bufferpool
	// actually adds them to the clock. Must be replaced for other replace policy
	// Also, let's define a lambda expr here
	getClockSize := func() int { return (*bpm.replacePolicy.clock).GetSize() }
	AssertEqual(t, getClockSize(), 1)

	//Buffer is full, clock has unpinned(pincount=0) node(id=2), try to get a new node
	bpm.GetNewNode() // id=4
	AssertEqual(t, (*bpm.pool[2]).getID(), NodeID(10))

	//Size of clock again, now 0 (should have been removed from Clock)
	AssertEqual(t, getClockSize(), 0)

	// Node 200 doesn't exist, shouldn't return nil but an error instead
	AssertNotEqual(t, bpm.UnpinNode(200, false), nil)

	// Pool is ful, also all nodes are in use, so nothing to evict
	var nilnode *node
	AssertEqual(t, bpm.GetNewNode(), nilnode)

	// Put node(id=1) into the clock
	bpm.UnpinNode(1, false)
	AssertEqual(t, getClockSize(), 1)

	// Now fetch it (from buffer pool) (and hence remove it from the clock again)
	bpm.FetchNode(1)
	AssertEqual(t, getClockSize(), 0)

	bpm.FetchNode(1)
	bpm.FetchNode(1)
	bpm.FetchNode(1)
	// Counter should be at 4 now
	AssertEqual(t, (*bpm.pool[1]).getPinCounter(), uint64(4))
	(*bpm.pool[1]).setPinCounter(1) //back to 1

	// Unpin all nodes with id 0,1,4,3 (remember, node id=2 got replaced with id=4)
	AssertEqual(t, bpm.UnpinNode(0, false), nil)
	AssertEqual(t, bpm.UnpinNode(1, false), nil)
	AssertEqual(t, bpm.UnpinNode(4, false), nil)
	AssertEqual(t, bpm.UnpinNode(3, false), nil)
	AssertEqual(t, getClockSize(), 4) //should be 4 by now

	//Replace our old nodes 1...4\{2} with 5...8 with new ones
	// 1..4\{2} should be stored to disk now
	bpm.GetNewNode() //id=5
	bpm.GetNewNode() //6
	bpm.GetNewNode() //7
	bpm.GetNewNode() //8

	//Unpin node 6,7
	AssertEqual(t, bpm.UnpinNode(6, false), nil)
	AssertEqual(t, bpm.UnpinNode(7, false), nil)

	// Test fetch from disk
	bpm.FetchNode(1)
	bpm.FetchNode(2)
	AssertEqual(t, (*bpm.pool[1]).getID(), NodeID(12)) //put into frame 2 and 3, since we unpinned 6,7 wich was in there
	AssertEqual(t, (*bpm.pool[2]).getID(), NodeID(10))

	// Try to delete node that's in use
	AssertNotEqual(t, bpm.DeleteNode(2), nil) // will not work and throw error
	bpm.UnpinNode(2, false)
	//Now it should work
	AssertEqual(t, bpm.DeleteNode(2), nil)

	AssertEqual(t, bpm.FetchNode(200), nilnode)  // node ID doesn't exist
	AssertNotEqual(t, bpm.FetchNode(2), nilnode) // node used to exist.. and still is in buffer pool... *feature* (should be AsserEqual for it to be correct)

	bpm.UnpinNode(2, false)

	bpm.FlushNode(5) // Check visually....
	bpm.FlushAllNodes()


}
