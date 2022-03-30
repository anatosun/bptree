package bm

import (
	"fmt"
	"testing"
)

func TestInitBPM(t *testing.T) {

	clock := NewClockPolicy(BufferPoolCapacity)
	disk := NewDiskManagerMock()
	bpm := NewBufferPoolManager(disk, clock)
	//clockReplacer := bpm.NewClockReplacer(bpm.MaxPoolSize)

	fmt.Println("Get 4 new pages...")
	bpm.GetNewPage()
	bpm.GetNewPage()
	bpm.GetNewPage()
	bpm.GetNewPage()

	fmt.Println("Unpin page with id=0")
	bpm.UnpinPage(0, true)

	fmt.Println("\n\n\nPages currently in Buffer Pool:")
	bpm.PrintPages()

	fmt.Println("\n\n\nPages currently in Clock:") // should be 1
	clock.clock.Print()

	fmt.Println("Buffer is full, clock has unpinned page, try to get a new page:")
	bpm.GetNewPage()

	fmt.Println("\n\n\nPages currently in Buffer Pool:")
	bpm.PrintPages()

	fmt.Println("\n\n\nPages currently in Clock:") // should be 0
	clock.clock.Print()

	fmt.Println("Unpin page with id=4")
	bpm.UnpinPage(4, true)

	fmt.Println("Unpin page with id=2")
	bpm.UnpinPage(2, true)

	fmt.Println("\n\n\nPages currently in Clock:") // should be 0 and 2
	clock.clock.Print()

	fmt.Println("try to get 2 new pages:")
	bpm.GetNewPage()

	fmt.Println("\n\n\nPages currently in Clock:") // should be 4 and 2
	clock.clock.Print()
}
