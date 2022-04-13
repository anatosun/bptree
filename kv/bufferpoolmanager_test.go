package kv

import (
	"testing"
	"fmt"
)

func TestBufferPoolManager(t *testing.T) {

	// clock := NewClockPolicy(BufferPoolCapacity)
	// disk := NewDiskManager(NodeID(0))
	// bpm := NewBufferPoolManager(disk, clock)

	// degree := uint8(3)

	// start := disk.numNode

	// const n int = 20//asume n<=Bufferpooolcapacity, don't change this since the tests are bound to this! (order, sitze..)

	// // Get 4 new nodes
	// for i := 0; i < n; i++ {
	// 	bpm.GetNewNode(degree) //id=i+start
	// }

	// AssertEqual(t, len(bpm.pool), BufferPoolCapacity)

	// // Unpin node with id=2 and set dirty bit
	// testingNode, _ := bpm.FetchNode(2)
	// AssertEqual(t, testingNode.getPinCounter(), uint64(1))
	// AssertEqual(t, bpm.UnpinNode(testingNode.getID()), nil) //if not nill => error
	// AssertEqual(t, (*bpm.pool[2]).getPinCounter(), uint64(0))
	// // AssertEqual(t, (*bpm.pool[2]).IsDirty(), true) //Needs to be updated...

	// // Nodes currently in Clock, should be 1
	// // Very specific to the clock, but to test whether bufferpool
	// // actually adds them to the clock. Must be replaced for other replace policy
	// // Also, let's define a lambda expr here
	// // getClockSize := func() int { return (*bpm.replacePolicy.clock).GetSize() }
	// // AssertEqual(t, getClockSize(), 1)

	// //Buffer is full, clock has unpinned(pincount=0) node( (*bpm.pool[2]) ), try to get a new node
	// testingNodeID, _ := bpm.GetNewNode(degree) //id=21
	// testingNode, _ = bpm.FetchNode(*testingNodeID)
	// AssertEqual(t, testingNode.getID(), NodeID(start+20))

	// //Size of clock again, now 0 (should have been removed from Clock)
	// // AssertEqual(t, getClockSize(), 0)

	// // Node 200 doesn't exist, shouldn't return nil but an error instead
	// AssertNotEqual(t, bpm.UnpinNode(200), nil)

	// // // Pool is full, also all nodes are in use, so nothing to evict, should return error
	// // _, err := bpm.GetNewNode(degree)
	// // AssertNotEqual(t, err, nil)

	// // Put node(id=11) into the clock
	// // testingNode = bpm.FetchNode(11)
	// // bpm.UnpinNode((*testingNode).getID())
	// // AssertEqual(t, getClockSize(), 1)



	// // // Now fetch it (from buffer pool) (and hence remove it from the clock again)
	// // bpm.FetchNode((*node).getID())
	// // AssertEqual(t, getClockSize(), 0)

	// // bpm.FetchNode((*node).getID())
	// // bpm.FetchNode((*node).getID())
	// // bpm.FetchNode((*node).getID())

	// // // Counter should be at 4 now
	// // AssertEqual(t, (*node).getPinCounter(), uint64(4))
	// // (*bpm.pool[1]).setPinCounter(1) //back to 1


	// // // Unpin all nodes with id 0,1,4,3 (+start) (remember, node id=2 got replaced with id=4)
	// // AssertEqual(t, bpm.UnpinNode(NodeID(start)), nil)
	// // AssertEqual(t, bpm.UnpinNode(NodeID(start+1)), nil)
	// // AssertEqual(t, bpm.UnpinNode(NodeID(start+4)), nil)
	// // AssertEqual(t, bpm.UnpinNode(NodeID(start+3)), nil)
	// // AssertEqual(t, getClockSize(), 4) //should be 4 by now



	// // //Replace our old nodes 1...4\{2} with 5...8 with new ones
	// // // 1..4\{2} should be stored to disk now
	// // bpm.GetNewNode(degree) //id=5
	// // bpm.GetNewNode(degree) //6
	// // bpm.GetNewNode(degree) //7
	// // bpm.GetNewNode(degree) //8

	// // // Unpin node 6,7
	// // AssertEqual(t, bpm.UnpinNode(NodeID(start+6)), nil)
	// // AssertEqual(t, bpm.UnpinNode(NodeID(start+7)), nil)

	// // // Test fetch from disk
	// // bpm.FetchNode(NodeID(start+1))
	// // bpm.FetchNode(NodeID(start+2))

	// // AssertEqual(t, (*bpm.pool[1]).getID(), NodeID(start+12)) //put into frame 2 and 3, since we unpinned 6,7 wich was in there
	// // AssertEqual(t, (*bpm.pool[2]).getID(), NodeID(start+10))

	// // // Try to delete node that's in use
	// // AssertNotEqual(t, bpm.DeleteNode(NodeID(start+2)), nil) // will not work and throw error
	// // bpm.UnpinNode(NodeID(start+2))
	// // //Now it should work
	// // AssertEqual(t, bpm.DeleteNode(NodeID(start+2)), nil)

	// // AssertEqual(t, bpm.FetchNode(NodeID(start+200)), nilnode)  // node ID doesn't exist
	// // AssertNotEqual(t, bpm.FetchNode(NodeID(start+2)), nilnode) // node used to exist.. and still is in buffer pool... *feature* (should be AsserEqual for it to be correct)

	// // bpm.UnpinNode(NodeID(start+2))

	// // bpm.FlushNode(NodeID(start+5)) // Check visually....
	// // bpm.FlushAllNodes()


}

func dummyfmt2() {
	fmt.Println("x")
}
