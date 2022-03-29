// PageID is the type of the page identifier
type PageID int

const pageSize = 5

// Page represents a page on disk
type Page struct {
	id       PageID
	pinCount int
	isDirty  bool
	data     [pageSize]byte
}

