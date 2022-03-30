package bm

import (
	"errors"
	"fmt"
)

const BufferPoolCapacity = 4

type BufferPoolManager struct {
	diskManager DiskManager
	pages       [BufferPoolCapacity]*Page
	replacer    *ClockPolicy
	freeList    []int
	pageTable   map[int]int
}

func NewBufferPoolManager(DiskManager DiskManager, clock *ClockPolicy) *BufferPoolManager {
	freeList := make([]int, 0)
	pages := [BufferPoolCapacity]*Page{}
	for i := 0; i < BufferPoolCapacity; i++ {
		freeList = append(freeList, int(i))
		pages[int(i)] = nil
	}
	return &BufferPoolManager{DiskManager, pages, clock, freeList, make(map[int]int)}
}

func (bpm *BufferPoolManager) GetNewPage() *Page {
	frameID, isFromFreeList := bpm.GetFrameID()
	if frameID == nil {
		return nil
	}

	// Victimized, i.e. not from free list
	if !isFromFreeList {
		page := bpm.pages[*frameID]
		if page.IsDirty() {
			// save to disk
			bpm.diskManager.WritePage(page)
			page.dirty = false
		}

		//remove page from frame
		delete(bpm.pageTable, page.id)

		//fmt.Println("Page not from free list")
	} else {
		//fmt.Println("Page from free list")
	}

	// allocate new page

	id := bpm.diskManager.AllocatePage()
	page := &Page{id: *id, data: [pageSize]byte{}, dirty: false, pinCounter: 1}
	bpm.pageTable[*id] = *frameID
	bpm.pages[*frameID] = page

	// return Page
	return page
}

func (bpm *BufferPoolManager) GetFrameID() (*int, bool) {
	if len(bpm.freeList) > 0 {
		frameID := bpm.freeList[0]
		bpm.freeList = bpm.freeList[1:]
		return &frameID, true
	}

	return (*bpm.replacer).Victim(), false
}

func (bpm *BufferPoolManager) UnpinPage(pageID int, dirty bool) error {
	// Unpin page by decreasing counter.
	// If counter == 0 => put into replacer for eviction
	// If isDirty is true, then set dirtybit to true

	frameID, found := bpm.pageTable[pageID]

	if !found {
		return errors.New("Page doesn't exist")
	}
	page := bpm.pages[frameID]
	page.decreasePinCounter()

	if page.getPinCounter() <= 0 {
		(*bpm.replacer).Unpin(frameID)
	}

	if page.IsDirty() || dirty {
		page.dirty = true
	}

	return nil
}

func (bpm *BufferPoolManager) PrintPages() {
	for _, page := range bpm.pages {
		page.Print()
	}
}

func (bpm *BufferPoolManager) FetchPage(pageID int) *Page {
	// Fetch page with given pageID,

	//check first if page is in buffer pool, if yes, return
	frameID, found := bpm.pageTable[pageID]

	if found {
		page := bpm.pages[frameID]
		page.increasePinCounter()

		(*bpm.replacer).Pin(frameID) // remove page from clock
		return page

		fmt.Printf("pageID=%d,frameID=%d exists in buffer pool\n", pageID, frameID)
	} else {
		// Page doesn't exist in buffer pool,
		// time to load it

		fmt.Printf("pageID=%d doesn't exists in buffer pool\n", pageID)
	}

	// first get a free frameID
	freeFrameID, isFromFreeList := bpm.GetFrameID()

	// Victimized, i.e. not from free list
	if !isFromFreeList {
		page := bpm.pages[*freeFrameID]
		if page.IsDirty() {
			// save to disk
			bpm.diskManager.WritePage(page)
			page.dirty = false
		}

		//remove page from frame
		delete(bpm.pageTable, page.id)

		//fmt.Println("Page not from free list")
	} else {
		//fmt.Println("Page from free list")

	}

	page, err := bpm.diskManager.ReadPage(pageID)
	if err != nil {
		return nil
	}

	(*page).setPinCounter(1)
	bpm.pageTable[pageID] = *freeFrameID
	bpm.pages[*freeFrameID] = page

	return page
}

func (bpm *BufferPoolManager) DeletePage(pageID int) error {

	frameID, found := bpm.pageTable[pageID]

	if !found {
		return errors.New("Page doesn't exist")
	}

	page := bpm.pages[frameID]

	if !page.hasZeroPins() {
		return errors.New("Page is still in use, cannot be deleted")
	}

	delete(bpm.pageTable, page.id)
	bpm.diskManager.DeallocatePage(pageID)
	bpm.freeList = append(bpm.freeList, frameID)

	// Note: The page will still stay inside of the pageTable until it has been replaced

	return nil
}

func (bpm *BufferPoolManager) FlushPage(pageID int) bool {
	frameID, found := bpm.pageTable[pageID]
	if found {
		page := bpm.pages[frameID]
		bpm.diskManager.WritePage(page)
		page.dirty = false //written to disk, i.e. up to date
		return true
	}
	return false
}

func (bpm *BufferPoolManager) FlushAllPages() {
	for id := range bpm.pageTable {
		bpm.FlushPage(id)
	}
}
