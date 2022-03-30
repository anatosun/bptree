package bm

import (
	"errors"
	"fmt"
)

const BufferPoolCapacity = 4

type BufferPoolManager struct {
	diskManager    DiskManager
	pages          [BufferPoolCapacity]*Page
	replacePolicy  *ClockPolicy
	freeFramesList []int
	pagesTable     map[int]int
}

func NewBufferPoolManager(DiskManager DiskManager, clock *ClockPolicy) *BufferPoolManager {
	freeFramesList := make([]int, 0)
	pages := [BufferPoolCapacity]*Page{}
	for i := 0; i < BufferPoolCapacity; i++ {
		freeFramesList = append(freeFramesList, int(i))
		pages[int(i)] = nil
	}
	return &BufferPoolManager{DiskManager, pages, clock, freeFramesList, make(map[int]int)}
}

func (bpm *BufferPoolManager) GetNewPage() *Page {
	frameID, isFromFreeFramesList := bpm.GetFrameID()
	if frameID == nil {
		return nil
	}

	// Victimized, i.e. not from free list
	if !isFromFreeFramesList {
		page := bpm.pages[*frameID]
		if page.IsDirty() {
			// save to disk
			bpm.diskManager.WritePage(page)
			page.dirty = false
		}

		//remove page from frame
		delete(bpm.pagesTable, page.id)

		//fmt.Println("Page not from free list")
	} else {
		//fmt.Println("Page from free list")
	}

	// allocate new page

	id := bpm.diskManager.AllocatePage()
	page := &Page{id: *id, data: [pageSize]byte{}, dirty: false, pinCounter: 1}
	bpm.pagesTable[*id] = *frameID
	bpm.pages[*frameID] = page

	// return Page
	return page
}

func (bpm *BufferPoolManager) GetFrameID() (*int, bool) {
	if len(bpm.freeFramesList) > 0 {
		frameID := bpm.freeFramesList[0]
		bpm.freeFramesList = bpm.freeFramesList[1:]
		return &frameID, true
	}

	return (*bpm.replacePolicy).Victim(), false
}

func (bpm *BufferPoolManager) UnpinPage(pageID int, dirty bool) error {
	// Unpin page by decreasing counter.
	// If counter == 0 => put into replacePolicy for eviction
	// If isDirty is true, then set dirtybit to true

	frameID, found := bpm.pagesTable[pageID]

	if !found {
		return errors.New("Page doesn't exist")
	}
	page := bpm.pages[frameID]
	page.decreasePinCounter()

	if page.getPinCounter() <= 0 {
		(*bpm.replacePolicy).Unpin(frameID)
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
	frameID, found := bpm.pagesTable[pageID]

	if found {
		page := bpm.pages[frameID]
		page.increasePinCounter()

		(*bpm.replacePolicy).Pin(frameID) // remove page from clock
		return page

		fmt.Printf("pageID=%d,frameID=%d exists in buffer pool\n", pageID, frameID)
	} else {
		// Page doesn't exist in buffer pool,
		// time to load it

		fmt.Printf("pageID=%d doesn't exists in buffer pool\n", pageID)
	}

	// first get a free frameID
	freeFrameID, isFromFreeFramesList := bpm.GetFrameID()

	// Victimized, i.e. not from free list
	if !isFromFreeFramesList {
		page := bpm.pages[*freeFrameID]
		if page.IsDirty() {
			// save to disk
			bpm.diskManager.WritePage(page)
			page.dirty = false
		}

		//remove page from frame
		delete(bpm.pagesTable, page.id)

		//fmt.Println("Page not from free list")
	} else {
		//fmt.Println("Page from free list")

	}

	page, err := bpm.diskManager.ReadPage(pageID)
	if err != nil {
		return nil
	}

	(*page).setPinCounter(1)
	bpm.pagesTable[pageID] = *freeFrameID
	bpm.pages[*freeFrameID] = page

	return page
}

func (bpm *BufferPoolManager) DeletePage(pageID int) error {

	frameID, found := bpm.pagesTable[pageID]

	if !found {
		return errors.New("Page doesn't exist")
	}

	page := bpm.pages[frameID]

	if !page.hasZeroPins() {
		return errors.New("Page is still in use, cannot be deleted")
	}

	delete(bpm.pagesTable, page.id)
	bpm.diskManager.DeallocatePage(pageID)
	bpm.freeFramesList = append(bpm.freeFramesList, frameID)

	// Note: The page will still stay inside of the pagesTable until it has been replaced

	return nil
}

func (bpm *BufferPoolManager) FlushPage(pageID int) bool {
	frameID, found := bpm.pagesTable[pageID]
	if found {
		page := bpm.pages[frameID]
		bpm.diskManager.WritePage(page)
		page.dirty = false //written to disk, i.e. up to date
		return true
	}
	return false
}

func (bpm *BufferPoolManager) FlushAllPages() {
	for id := range bpm.pagesTable {
		bpm.FlushPage(id)
	}
}
