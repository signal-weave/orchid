package storage

import (
	"encoding/binary"

	"orchiddb/globals"
)

const (
	MetaPageNum     pageNum = 0
	FreelistPageNum pageNum = 1
	RootNodePageNum pageNum = 2
)

// The database file table of contents.
// Contains page numbers for various non-user-created pages, such as the
// freelist and root node pages.
// Should always default to page 0 in a new db file.
type meta struct {
	FreelistPageNum pageNum
	RootPageNum     pageNum
}

func newMeta() *meta {
	return &meta{
		FreelistPageNum: FreelistPageNum,
		RootPageNum:     RootNodePageNum,
	}
}

// serializeToPage writes the meta's contents into page p.
func (m *meta) serializeToPage() *page {
	p := newEmptyPage(MetaPageNum)
	pos := 0

	insertPageMarker(p.contents)
	pos += globals.PageMarkerSize

	binary.LittleEndian.PutUint64(p.contents[pos:], uint64(m.FreelistPageNum))
	pos += globals.PageNumSize

	binary.LittleEndian.PutUint64(p.contents[pos:], uint64(m.RootPageNum))
	pos += globals.PageNumSize

	return p
}

// deserializeFromPage constructs a new meta from the contents of page p.
func (m *meta) deserializeFromPage(p *page) {
	pos := 0

	verifyPageMarker(p.contents)
	pos += globals.PageMarkerSize

	m.FreelistPageNum = pageNum(binary.LittleEndian.Uint64(p.contents[pos:]))
	pos += globals.PageNumSize

	m.RootPageNum = pageNum(binary.LittleEndian.Uint64(p.contents[pos:]))
	pos += globals.PageNumSize
}
