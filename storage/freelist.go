package storage

import (
	"encoding/binary"

	"orchiddb/globals"
)

// metaPage is the maximum pgnum that is used by the db for its own purposes.
// For now, only page 0 is used as the header page.
// It means all other page numbers can be used.
const metaPage = 0

// The freelist is responsible for assigning the next available page, either by
// using the space of a freed page or allocating a new one.
type freelist struct {
	// Holds the maximum page allocated. maxpage*pageSize = filesize
	MaxPage pageNum

	// Pages that were previously allocated but are now free
	ReleasedPages []pageNum
}

func newFreelist() *freelist {
	return &freelist{
		MaxPage:       metaPage,
		ReleasedPages: []pageNum{},
	}
}

// -------Page Management-------------------------------------------------------

// getNextPage returns page ids for writing.
// New page ids are first given from the releasedPageIDs to avoid growing the
// file. If it's empty, then maxPage is incremented and a new page is created
// thus increasing the file size.
func (fr *freelist) GetNextPage() pageNum {
	// Take the last element and remove it from the list
	if len(fr.ReleasedPages) != 0 {
		pageID := fr.ReleasedPages[len(fr.ReleasedPages)-1]
		fr.ReleasedPages = fr.ReleasedPages[:len(fr.ReleasedPages)-1]
		return pageID
	}

	fr.MaxPage += 1
	return fr.MaxPage
}

func (fr *freelist) ReleasePage(page pageNum) {
	fr.ReleasedPages = append(fr.ReleasedPages, page)
}

// -------Serialization---------------------------------------------------------

// serializeToPage writes the freelist's contents into page p.
func (fr *freelist) serializeToPage() *page {
	p := NewEmptyPage(FreelistPageNum)
	pos := 0

	// Page marker
	insertPageMarker(p.contents)
	pos += globals.PageMarkerSize

	// MaxPage count
	binary.LittleEndian.PutUint64(p.contents[pos:], uint64(fr.MaxPage))
	pos += 8

	// released pages count
	binary.LittleEndian.PutUint16(p.contents[pos:], uint16(len(fr.ReleasedPages)))
	pos += 2

	for _, page := range fr.ReleasedPages {
		binary.LittleEndian.PutUint64(p.contents[pos:], uint64(page))
		pos += globals.PageNumSize
	}

	return p
}

// deserializeFromPage constructs a new freelist from the contents of page p.
func (fr *freelist) deserializeFromPage(p *page) {
	pos := 0
	fr.ReleasedPages = fr.ReleasedPages[:0] // reset

	// Page marker
	verifyPageMarker(p.contents)
	pos += globals.PageMarkerSize

	// Max page count
	fr.MaxPage = pageNum(binary.LittleEndian.Uint64(p.contents[pos:]))
	pos += 8

	// Release page count
	releasedPagesCount := int(binary.LittleEndian.Uint16(p.contents[pos:]))
	pos += 2

	for range releasedPagesCount {
		page := pageNum(binary.LittleEndian.Uint64(p.contents[pos:]))
		pos += globals.PageNumSize
		fr.ReleasedPages = append(fr.ReleasedPages, page)
	}
}
