package storage

import (
	"encoding/binary"
	"fmt"
	"orchiddb/globals"
	"os"
)

const (
	pageNumDelimiter  = byte('d')
	pageNumTerminator = byte('t')

	// (4KiB - 4byte page marker - 1byte terminator) // (8byte page number + 1byte delimiter) = 454
	maxPageNumbers = 454
)

type WAL struct {
	pages []*page
}

func NewWal() *WAL {
	return &WAL{
		pages: []*page{},
	}
}

func (w *WAL) reset() {
	w.pages = []*page{}
}

func (w *WAL) appendPage(p *page) {
	w.pages = append(w.pages, p)
}

func (w *WAL) WriteLog(path string) error {
	if len(w.pages) == 0 {
		return fmt.Errorf("WAL has no pages to write.")
	}

	_, err := os.Stat(path)
	if err == nil {
		return fmt.Errorf("WAL file for %s already exists!", path)
	}

	walFile, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}

	var out []byte = []byte{}
	out = append(out, w.serializeWalMetaPage()...)
	for _, pg := range w.pages {
		out = append(out, pg.contents...)
	}
	out = append(out, globals.WalSuccessMarker...)

	_, err = walFile.WriteAt(out, 0)
	if err != nil {
		return fmt.Errorf("unable to write wal to %s", path)
	}

	return nil
}

// serializeWalMetaPage returns a byte array page contents of the uint64 page
// numbers that are in the WAL file.
// Like all pages, this page beings with a page marker for validity.
func (w *WAL) serializeWalMetaPage() []byte {
	out := make([]byte, globals.PageSize)
	insertPageMarker(out)

	for _, p := range w.pages {
		binary.LittleEndian.PutUint64(out, uint64(p.pageNum))
	}

	return out
}
