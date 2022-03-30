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

	fmt.Println("Unpin page with id=3")
	fmt.Println(bpm.UnpinPage(3, false))

	bpm.PrintPages()

	fmt.Println("Unpin page with id=200 (doesn't exist)")
	err := bpm.UnpinPage(200, true)
	if err == nil {
		fmt.Println("page found...")
	} else {
		fmt.Println("page doesn't exist")
	}

	fmt.Println("\n\n\nPages currently in Clock:") // should be 3
	clock.clock.Print()

	fmt.Println("try to get 1 new pages:")
	bpm.GetNewPage()

	fmt.Println("\n\n\nPages currently in Clock:")
	clock.clock.Print()

	fmt.Println("\n\n\nPages currently in Buffer Pool:")
	bpm.PrintPages()

	///////

	bpm.UnpinPage(1, false)
	bpm.UnpinPage(1, false)

	fmt.Println("\n\n\nPages currently in Clock:")
	clock.clock.Print()

	bpm.FetchPage(1)
	bpm.FetchPage(1)
	bpm.FetchPage(1)
	bpm.FetchPage(1)

	// counter should be 4 now for page.id=4

	fmt.Println("\n\n\nPages currently in Clock:")
	clock.clock.Print()

	fmt.Println("\n\n\nPages currently in Buffer Pool:")
	bpm.PrintPages()

	//// test fetch from disk
	bpm.UnpinPage(5, false)

	fmt.Println("\n\n\nPages currently in Buffer Pool:")
	bpm.PrintPages()

	fmt.Println("\n\n\nget page with ID=0")
	bpm.FetchPage(0)
	bpm.PrintPages()

	// Try to delete page
	fmt.Println(bpm.DeletePage(2)) // will not work and throw error

	fmt.Println("\n\n\nPages currently in Buffer Pool:")
	bpm.PrintPages()

	bpm.UnpinPage(2, false)

	//Now it should work
	fmt.Println(bpm.DeletePage(2))
	fmt.Println("\n\n\nPages currently in Buffer Pool:")
	bpm.PrintPages()

	bpm.GetNewPage()
	fmt.Println("\n\n\nPages currently in Buffer Pool:")
	bpm.PrintPages()

	bpm.UnpinPage(0, true)
	bpm.FetchPage(2)

	fmt.Println("\n\n\nPages currently in Buffer Pool:")
	bpm.PrintPages()

	bpm.FlushAllPages()

	bpm.PrintPages()

}
