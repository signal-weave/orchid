package storage

import (
	"errors"
	"io"
	"os"

	"orchiddb/globals"
)

type Pager struct {
	f *os.File
}

func OpenPager(path string) (*Pager, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	return &Pager{f: f}, nil
}

func (p *Pager) Close() error {
	return p.f.Close()
}

func (p *Pager) Sync() error {
	return p.f.Sync()
}

func (p *Pager) ReadPage(num pageNum) (*page, error) {
	offset := int64(num) * int64(globals.PageSize)
	buf := make([]byte, globals.PageSize)

	n, err := p.f.ReadAt(buf, offset)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	if n != globals.PageSize {
		// If file is shorter, the unread portion should be zeroed; our buffer
		// already is.
	}

	pg := NewEmptyPage(num)
	pg.contents = buf
	return pg, nil
}

func (p *Pager) WritePage(pg *page) error {
	if len(pg.contents) != globals.PageSize {
		return errors.New("page size mismatch")
	}

	offset := int64(pg.pageNum) * int64(globals.PageSize)
	_, err := p.f.WriteAt(pg.contents, offset)

	return err
}
