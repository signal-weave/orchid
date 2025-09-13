package storage

import (
	"encoding/binary"
)

const MetaPageNum pageNum = 0 // The meta page's page number in each db file
const FreelistPageNum pageNum = 1
const RootNodePageNum pageNum = 2

// The database file table of contents.
// Contains page numbers for various non-user created pages, such as the
// freelist and root node pages.
// Should always default to page 0 in a new db file.
type meta struct {
	FreelistPage pageNum
	RootPage     pageNum
}

func newMeta() *meta {
	return &meta{
		FreelistPage: FreelistPageNum,
		RootPage:     RootNodePageNum,
	}
}

// serializeToPage writes the meta's contents into page p.
func (m *meta) serializeToPage(p *page) {
	buf := p.contents

	pos := 0

	binary.LittleEndian.PutUint64(buf[pos:], uint64(m.FreelistPage))
	pos += PageNumSize

	binary.LittleEndian.PutUint64(buf[pos:], uint64(m.RootPage))
	pos += PageNumSize
}

// deserializeFromPage constructs a new meta from the contents of page p.
func (m *meta) deserializeFromPage(p *page) {
	buf := p.contents

	pos := 0

	m.FreelistPage = pageNum(binary.LittleEndian.Uint64(buf[pos:]))
	pos += PageNumSize

	m.RootPage = pageNum(binary.LittleEndian.Uint64(buf[pos:]))
	pos += PageNumSize
}
