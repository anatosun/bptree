package bm

import (
	"fmt"
)

//CONFIG: set here the desired pageSize in bytes
// 4 KB
const PageSize = 4 * 1000

type PageID uint32

type Page struct {
	id         PageID
	data       [PageSize]byte
	dirty      bool
	pinCounter uint64
}

func (page *Page) getID() PageID {
	return page.id
}

func (page *Page) getPinCounter() uint64 {
	return page.pinCounter
}

func (page *Page) setPinCounter(val uint64) {
	page.pinCounter = val
}

func (page *Page) hasZeroPins() bool {
	return page.pinCounter <= 0
}

func (page *Page) increasePinCounter() error {
	//possibly implement limit on pins here
	page.pinCounter++
	return nil
}

func (page *Page) decreasePinCounter() error {
	//possibly implement limit on pins here
	if page.pinCounter <= 0 {
		return fmt.Errorf("page.go: Counter is already zero")
	}

	page.pinCounter--

	return nil

}

func (page *Page) IsDirty() bool {
	return page.dirty
}

func (page *Page) Print() {
	fmt.Printf("page.id=%d\n", page.id)
	fmt.Printf("page.counter=%d\n", page.pinCounter)
	fmt.Printf("page.dirty=%t\n", page.dirty)
	fmt.Println("---------")
}
