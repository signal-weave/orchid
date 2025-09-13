package storage

import "orchiddb/globals"

const PageNumSize = 8    // The size of a page's number in bytes

type pageNum uint64

// A page is a 4Kib container that can be structured for either free lists,
// file header data, or the individual nodes in a file.
//
// A page can contain a node, with its items and child pointers, or it could
// contain a bespoke layout for specific tracking or metadata within the opened
// table file.
type page struct {
	pageNum  pageNum
	contents []byte
}

func NewEmptyPage(pagenum pageNum) *page {
	contents := make([]byte, globals.PageSize)

	return &page{
		pageNum:  pageNum(pagenum),
		contents: contents,
	}
}
