package storage

import (
	"encoding/binary"

	"orchiddb/globals"
)

const (
	MetaPageNum     pageNum = 0 // The meta page's page number in each db file
	FreelistPageNum pageNum = 1
	RootNodePageNum pageNum = 2
)

// The database file table of contents.
// Contains page numbers for various non-user created pages, such as the
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
func (m *meta) serializeToPage(p *page) {
	pos := 0
	buf := p.contents

	insertPageMarker(buf)
	pos += globals.PageMarkerSize

	binary.LittleEndian.PutUint64(buf[pos:], uint64(m.FreelistPageNum))
	pos += globals.PageNumSize

	binary.LittleEndian.PutUint64(buf[pos:], uint64(m.RootPageNum))
	pos += globals.PageNumSize
}

// deserializeFromPage constructs a new meta from the contents of page p.
func (m *meta) deserializeFromPage(p *page) {
	pos := 0
	buf := p.contents

	verifyPageMarker(buf)
	pos += globals.PageMarkerSize

	m.FreelistPageNum = pageNum(binary.LittleEndian.Uint64(buf[pos:]))
	pos += globals.PageNumSize

	m.RootPageNum = pageNum(binary.LittleEndian.Uint64(buf[pos:]))
	pos += globals.PageNumSize
}
