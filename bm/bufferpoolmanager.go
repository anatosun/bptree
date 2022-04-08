package bm

import (
	"errors"
	"fmt"
)

const BufferPoolCapacity = 4
const debug_buffer = false

type BufferPoolManager struct {
	diskManager    DiskManager
	pool           [BufferPoolCapacity]*Page
	replacePolicy  *ClockPolicy
	freeFramesList []PageID
	pagesTable     map[PageID]PageID
}

func NewBufferPoolManager(DiskManager DiskManager, clock *ClockPolicy) *BufferPoolManager {
	freeFramesList := make([]PageID, 0)
	pages := [BufferPoolCapacity]*Page{}
	for i := 0; i < BufferPoolCapacity; i++ {
		freeFramesList = append(freeFramesList, PageID(i))
		pages[PageID(i)] = nil
	}
	return &BufferPoolManager{DiskManager, pages, clock, freeFramesList, make(map[PageID]PageID)}
}

func (bpm *BufferPoolManager) GetNewPage() *Page {
	frameID, isFromFreeFramesList := bpm.GetFrameID()
	if frameID == nil {
		return nil
	}

	// Victimized, i.e. not from free list
	if !isFromFreeFramesList {
		page := bpm.pool[*frameID]
		if page.IsDirty() {
			// save to disk
			bpm.diskManager.WritePage(page)
			page.dirty = false
			page.pinCounter = 0

		} else {
			// let's still save it to disk
			// since we're currently only emulating the disk
			bpm.diskManager.WritePage(page)
			page.dirty = false
			page.pinCounter = 0
		}

		//remove page from frame
		delete(bpm.pagesTable, PageID(page.id))

		//fmt.Println("Page not from free list")
	} else {
		//fmt.Println("Page from free list")
	}

	// allocate new page

	id := bpm.diskManager.AllocatePage()
	page := &Page{id: *id, data: [PageSize]byte{}, dirty: false, pinCounter: 1}
	bpm.pagesTable[*id] = *frameID
	bpm.pool[*frameID] = page

	// return Page
	return page
}

func (bpm *BufferPoolManager) GetFrameID() (*PageID, bool) {
	if len(bpm.freeFramesList) > 0 {
		frameID := bpm.freeFramesList[0]
		bpm.freeFramesList = bpm.freeFramesList[1:]
		return &frameID, true
	}

	return (*bpm.replacePolicy).Victim(), false
}

func (bpm *BufferPoolManager) UnpinPage(pageID PageID, dirty bool) error {
	// Unpin page by decreasing counter.
	// If isDirty is true, then set dirtybit to true

	frameID, found := bpm.pagesTable[pageID]

	if !found {
		return errors.New("Page doesn't exist")
	}
	page := bpm.pool[frameID]

	err := page.decreasePinCounter()

	if err != nil {
		return err
	}

	if page.getPinCounter() <= 0 {
		(*bpm.replacePolicy).Unpin(frameID)
	}

	if page.IsDirty() || dirty {
		page.dirty = true
	}

	return nil
}

func (bpm *BufferPoolManager) PrintPages() {
	fmt.Println("------------------------------------")
	fmt.Println("Pages in Buffer Pool:")
	for _, page := range bpm.pool {
		fmt.Printf("page id=%d, dirtybit=%t, counter=%d, content=%v\n", page.getID(), page.IsDirty(), page.getPinCounter(), page.data)
	}
}

func (bpm *BufferPoolManager) FetchPage(pageID PageID) *Page {
	// Fetch page with given pageID,

	//check first if page is in buffer pool, if yes, return
	frameID, found := bpm.pagesTable[pageID]

	if found {
		page := bpm.pool[frameID]
		page.increasePinCounter()

		(*bpm.replacePolicy).Pin(frameID) // remove page from clock
		return page

		if debug_buffer {
			fmt.Printf("pageID=%d,frameID=%d exists in buffer pool\n", pageID, frameID)
		}
	} else {
		// Page doesn't exist in buffer pool
		if debug_buffer {
			fmt.Printf("pageID=%d doesn't exists in buffer pool\n", pageID)
		}
	}

	// first get a free frameID
	freeFrameID, isFromFreeFramesList := bpm.GetFrameID()

	// If Victimized, i.e. not from free list, save to disk
	if !isFromFreeFramesList {
		page := bpm.pool[*freeFrameID]
		if page.IsDirty() {
			// save to disk
			bpm.diskManager.WritePage(page)
			page.dirty = false
			page.pinCounter = 0
		} else {
			// let's still save it to disk
			// since we're currently only emulating the disk
			bpm.diskManager.WritePage(page)
			page.dirty = false
			page.pinCounter = 0
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
	bpm.pool[*freeFrameID] = page

	return page
}

func (bpm *BufferPoolManager) DeletePage(pageID PageID) error {

	frameID, found := bpm.pagesTable[pageID]

	if !found {
		return errors.New("Page doesn't exist")
	}

	page := bpm.pool[frameID]

	if !page.hasZeroPins() {
		return errors.New("Page is still in use, cannot be deleted")
	}

	delete(bpm.pagesTable, page.id)
	bpm.diskManager.DeallocatePage(pageID)
	bpm.freeFramesList = append(bpm.freeFramesList, frameID)

	// Note: The page will still stay inside of the pagesTable until it has been replaced
	// by the page replacer.
	// Let's call this a feature.

	return nil
}

func (bpm *BufferPoolManager) FlushPage(pageID PageID) bool {
	frameID, found := bpm.pagesTable[pageID]
	if found {
		page := bpm.pool[frameID]
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
