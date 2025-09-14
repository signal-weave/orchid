package storage

import (
	"bytes"
	"errors"
	"orchiddb/globals"
)

type pageNum uint64

// A page is a 4Kib container that can be structured for either free lists,
// file header meta data, or the individual nodes in a file.
//
// A page can contain a node, with its items and child pointers, or it could
// contain a bespoke layout for specific tracking or metadata within the opened
// table file.
type page struct {
	magicMarker []byte
	pageNum     pageNum
	contents    []byte
}

func NewEmptyPage(pagenum pageNum) *page {
	contents := make([]byte, globals.PageSize)

	return &page{
		magicMarker: globals.PageMarker,
		pageNum:     pageNum(pagenum),
		contents:    contents,
	}
}

// Verifies that the page has the magic marker at the beginning.
// The 'magic marker' is 4 bytes that only orchid db pages should start with.
// It would be a huge coincidence if another program wrote orchid's byte marker
// at exactly the offset orchid is reading from.
//
// If the marker is not found, either a non-orchid page is being read or the
// pages have been offset or drifted, resulting in database corruption.
func verifyPageMarker(buf []byte) {
	marker := []byte{buf[0], buf[1], buf[2], buf[3]}
	if !bytes.Equal(marker, globals.PageMarker) {
		err := errors.New("Page marker not found, table pages are offset!")
		panic(err)
	}
}

// insertPageMarker appends globals.PageMarker to the beginning of a page.
// See verifyPageMarker doc for more details.
func insertPageMarker(buf []byte) {
	copy(buf[0:4], globals.PageMarker[:])
}
