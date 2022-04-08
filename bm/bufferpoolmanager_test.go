package bm

import (
	"testing"
)

func TestBufferPoolManager(t *testing.T) {

	clock := NewClockPolicy(BufferPoolCapacity)
	disk := NewDiskManagerMock()
	bpm := NewBufferPoolManager(disk, clock)

	const n int = 4 //asume n<=Bufferpooolcapacity

	// Get 4 new pages
	for i := 0; i < n; i++ {
		bpm.GetNewPage() //id=i
	}

	AssertEqual(t, len(bpm.pool), n)

	// Unpin page with id=2 and set dirty bit
	AssertEqual(t, (*bpm.pool[2]).getPinCounter(), uint64(1))
	AssertEqual(t, bpm.UnpinPage(2, true), nil) //if not nill => error
	AssertEqual(t, (*bpm.pool[2]).getPinCounter(), uint64(0))
	AssertEqual(t, (*bpm.pool[2]).IsDirty(), true)

	// Pages currently in Clock, should be 1
	// Very specific to the clock, but to test whether bufferpool
	// actually adds them to the clock. Must be replaced for other replace policy
	// Also, let's define a lambda expr here
	getClockSize := func() int { return (*bpm.replacePolicy.clock).GetSize() }
	AssertEqual(t, getClockSize(), 1)

	//Buffer is full, clock has unpinned(pincount=0) page(id=2), try to get a new page
	bpm.GetNewPage() // id=4
	AssertEqual(t, (*bpm.pool[2]).getID(), PageID(4))

	//Size of clock again, now 0 (should have been removed from Clock)
	AssertEqual(t, getClockSize(), 0)

	// Page 200 doesn't exist, shouldn't return nil but an error instead
	AssertNotEqual(t, bpm.UnpinPage(200, false), nil)

	// Pool is ful, also all pages are in use, so nothing to evict
	var nilpage *Page
	AssertEqual(t, bpm.GetNewPage(), nilpage)

	// Put page(id=1) into the clock
	bpm.UnpinPage(1, false)
	AssertEqual(t, getClockSize(), 1)

	// Now fetch it (from buffer pool) (and hence remove it from the clock again)
	bpm.FetchPage(1)
	AssertEqual(t, getClockSize(), 0)

	bpm.FetchPage(1)
	bpm.FetchPage(1)
	bpm.FetchPage(1)
	// Counter should be at 4 now
	AssertEqual(t, (*bpm.pool[1]).getPinCounter(), uint64(4))
	(*bpm.pool[1]).setPinCounter(1) //back to 1

	// Unpin all pages with id 0,1,4,3 (remember, page id=2 got replaced with id=4)
	AssertEqual(t, bpm.UnpinPage(0, false), nil)
	AssertEqual(t, bpm.UnpinPage(1, false), nil)
	AssertEqual(t, bpm.UnpinPage(4, false), nil)
	AssertEqual(t, bpm.UnpinPage(3, false), nil)
	AssertEqual(t, getClockSize(), 4)

	//Replace our old pages 1...4\{2} with 5...8 with new ones
	// 1..4\{2} should be stored to disk now
	bpm.GetNewPage() //id=5
	bpm.GetNewPage() //6
	bpm.GetNewPage() //7
	bpm.GetNewPage() //8

	//Unpin page 6,7
	AssertEqual(t, bpm.UnpinPage(6, false), nil)
	AssertEqual(t, bpm.UnpinPage(7, false), nil)

	// Test fetch from disk
	bpm.FetchPage(1)
	bpm.FetchPage(2)
	AssertEqual(t, (*bpm.pool[1]).getID(), PageID(1)) //put into frame 2 and 3, since we unpinned 6,7 wich was in there
	AssertEqual(t, (*bpm.pool[2]).getID(), PageID(2))

	// Try to delete page that's in use
	AssertNotEqual(t, bpm.DeletePage(2), nil) // will not work and throw error
	bpm.UnpinPage(2, false)
	//Now it should work
	AssertEqual(t, bpm.DeletePage(2), nil)

	AssertEqual(t, bpm.FetchPage(200), nilpage)  // page ID doesn't exist
	AssertNotEqual(t, bpm.FetchPage(2), nilpage) // page used to exist.. and still is in buffer pool... *feature* (should be AsserEqual for it to be correct)

	bpm.FlushPage(5) // Check visually....
	bpm.FlushAllPages()
}
