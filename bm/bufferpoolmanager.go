package bm

import (
//	"fmt"
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
	frameID, isFromFreeList := bpm.getFrameID()
	if frameID == nil {
		return nil
	}

	// Victimized, i.e. not from free list
	if !isFromFreeList {
		page := bpm.pages[*frameID]
		if page.IsDirty() {
			// save to disk
			bpm.diskManager.WritePage(page)
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

func (bpm *BufferPoolManager) getFrameID() (*int, bool) {
	if len(bpm.freeList) > 0 {
		frameID := bpm.freeList[0]
		bpm.freeList = bpm.freeList[1:]
		return &frameID, true
	}

	return (*bpm.replacer).Victim(), false
}

func (bpm *BufferPoolManager) UnpinPage(pageID int, dirty bool) {
	// Unpin page by decreasing counter.
	// If counter == 0 => put into replacer for eviction
	// If isDirty is true, then set dirtybit to true

	frameID := bpm.pageTable[pageID]
	page := bpm.pages[frameID]
	page.decreasePinCounter()

	if page.getPinCounter() <= 0 {
		(*bpm.replacer).Unpin(frameID)
	}

	if page.IsDirty() || dirty {
		page.dirty = true
	}

}

func (bpm *BufferPoolManager) PrintPages() {
	for _, page := range bpm.pages {
		page.Print()
	}
}

// func FetchPage(pageID PageID) *Page {
// 	// Fetch page with given pageID,
// 	// return Page
// }

// func FlushPage(pageID PageID) bool {
// 	// Check if dirtybit is set to 1, if yes, write to diskmanager (mocked)
// 	// Else, just flush
// }

// func FlushAllPages(){
// 	// Flush all pages
// }
