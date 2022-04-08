// This is a Mock Disk Manager, still saved to memory
// Source: https://github.com/brunocalza/buffer-pool-manager/blob/56e7b500fb3aa8884b71e8b478da909c0da1a918/disk_manager_mock.go
// Will be implemented correctly at some point later.. :)

package bm

import (
	"errors"
	"fmt"
)

//DiskMaxNumPages sets the disk capacity
const DiskMaxPagesCapacity = 20

// DiskManager is responsible for interacting with disk
type DiskManager interface {
	ReadPage(pageID PageID) (*Page, error)
	WritePage(*Page) error
	AllocatePage() *PageID
	DeallocatePage(pageID PageID)
}

//DiskManagerMock is a memory mock for disk manager
type DiskManagerMock struct {
	numPage int64 // tracks the number of pages. -1 indicates that there is no page, and the next to be allocates is 0
	pages   map[PageID]*Page
}

//ReadPage reads a page from pages
func (d *DiskManagerMock) ReadPage(pageID PageID) (*Page, error) {
	if page, ok := d.pages[pageID]; ok {
		return page, nil
	}

	return nil, errors.New("Page not found")
}

//WritePage writes a page in memory to pages
func (d *DiskManagerMock) WritePage(page *Page) error {
	d.pages[page.id] = page
	return nil
}

//AllocatePage allocates one more page
func (d *DiskManagerMock) AllocatePage() *PageID {
	if d.numPage == DiskMaxPagesCapacity-1 {
		return nil
	}
	d.numPage = d.numPage + 1
	pageID := PageID(d.numPage)
	return &pageID
}

//DeallocatePage removes page from disk
func (d *DiskManagerMock) DeallocatePage(pageID PageID) {
	delete(d.pages, pageID)
}

//NewDiskManagerMock returns a in-memory mock of disk manager
func NewDiskManagerMock() *DiskManagerMock {
	return &DiskManagerMock{-1, make(map[PageID]*Page)}
}

//Print pages
func (d *DiskManagerMock) PrintPages() {
	fmt.Println("------------------------------------")
	fmt.Println("Pages on disk:")
	for _, page := range d.pages {
		fmt.Printf("page id=%d, dirtybit=%t, counter=%d, content=%v\n", page.getID(), page.IsDirty(), page.getPinCounter(), page.data)
	}
}
