type BufferPoolManager struct {
	diskManager DiskManager
	pages       [MaxPoolSize]*Page
	replacer    *ClockReplacer
	freeList    []FrameID
	pageTable   map[PageID]FrameID
}


func NewBufferPoolManager(DiskManager DiskManager, clockReplacer *ClockReplacer) *BufferPoolManager {
	freeList := make([]FrameID, 0)
	pages := [MaxPoolSize]*Page{}
	for i := 0; i < MaxPoolSize; i++ {
		freeList = append(freeList, FrameID(i))
		pages[FrameID(i)] = nil
	}
	return &BufferPoolManager{DiskManager, pages, clockReplacer, freeList, make(map[PageID]FrameID)}
}




// NewPage() *Page
// FetchPage(pageID PageID) *Page
// FlushPage(pageID PageID) bool
// FlushAllPages()
// DeletePage(pageID PageID) error
// UnpinPage(pageID PageID, isDirty bool) error


func NewPage() *Page {
	// Ask diskmanager for a new page
	// return Page
	return nil
}

func FetchPage(pageID PageID) *Page {
	// Fetch page with given pageID, 
	// return Page
}

func FlushPage(pageID PageID) bool {
	// Check if dirtybit is set to 1, if yes, write to diskmanager (mocked)
	// Else, just flush
}

func FlushAllPages(){
	// Flush all pages
}

// Not sure if needed yet, let's put it here for reference
func DeletePage(pageID PageID) error {
}

func UnpinPage(pageID PageID, isDirty bool) error {
	// Unpin page by decreasing counter.
	// If counter == 0 => put into replacer for eviction
	// If isDirty is true, then set dirtybit to true
}

