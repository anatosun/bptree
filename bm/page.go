package bm

import (
	"fmt"
)

//CONFIG: set here the desired pageDataSize in bytes
// 4KB = 4096bytes in total

// Compute these values using pagesizes_calculator.go
// Desired Total Page Size: 4096
// Actual Total Page Size: 3687 (-10% from desired)
// Node Degree: 140

const PageDataSize = 3687

type PageID uint32

type Page struct {
	id         PageID
	data       [PageDataSize]byte
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
