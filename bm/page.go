package bm

import (
	"fmt"
)

//CONFIG: set here the desired pageSize in bytes
const pageSize = 256

type Page struct {
	id         int
	data       [pageSize]byte
	dirty      bool
	pinCounter int
}

func (page *Page) getID() int {
	return page.id
}

func (page *Page) getPinCounter() int {
	return page.pinCounter
}

func (page *Page) increasePinCounter() error {
	//possibly implement limit on pins here
	page.pinCounter++
	return nil
}

func (page *Page) decreasePinCounter() error {
	//possibly implement limit on pins here
	if page.pinCounter <= 0 {
		panic(fmt.Errorf("page.go: Counter is already zero"))
	} else {
		page.pinCounter--
		return nil
	}
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