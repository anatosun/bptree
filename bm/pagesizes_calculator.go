package bm

import(
	"fmt"
)

func ComputeLeafNodeSizeInBytes(degree int) int {
	// Size of a (leaf!) node is computed in the following way:
	//		degree * size(uint64)	// keysize
	//	  + degree * size(uint64)	// children
	//	  +      2 * size(uint64)	// next & previous links
	//	  + degree * [10]byte 		// 10 bytes for value

	// Which translates to the following numbers using
	// https://go.dev/ref/spec#Size_and_alignment_guarantees

	//		degree * 8
	//	  + degree * 8
	//	  +      2 * 8
	//	  + degree * 10

	return degree * 8 + degree * 8 + 2 * 8 + degree * 10
}


func ComputeNodeDegreeForBytes(sizeInBytes int) int {
	//rounds down
	return  (sizeInBytes-16)/26 //16 needed for prev and next link, 26 = (8+8+10)*degree (see above)
}

func ComputePageOverheadInBytes() int {
	//For the page we have the following format:
	
	// type Page struct {
	// 	8 	id         PageID //uint64
	// 	1 	dirty      bool
	// 	8 	pinCounter uint64
	// 	x 	data       [PageSize]byte
	// }

	return 8 + 1 + 8
}

func ComputePageSizesInBytes(desiredTotalPageSizeInBytes int) {
	// leave 10% empty
	actualTotalPageSize := desiredTotalPageSizeInBytes - int(0.1*float64(desiredTotalPageSizeInBytes))

	dataSize := actualTotalPageSize - ComputePageOverheadInBytes()

	nodeDegree := ComputeNodeDegreeForBytes(dataSize)

	fmt.Println("Computed Page Sizes:")
	fmt.Printf("Desired Total Page Size: %d\n", desiredTotalPageSizeInBytes)
	fmt.Printf("Actual Total Page Size: %d (-10%% from desired)\n", actualTotalPageSize)
	fmt.Printf("Page Data Size: %d (- Overhead for Page Meta Data)\n", dataSize)
	fmt.Printf("Node Degree: %d\n", nodeDegree)


}