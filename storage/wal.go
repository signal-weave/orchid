package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"orchiddb/globals"
	"os"
)

// A write-ahead-log (WAL) is a log detailing the actions that will be commited
// to the database before they happen.
//
// In the event of a power loss, if a WAL file is found for a table, it is read
// and there is an attempt to replay the logged node update action.
//
// When a WAL file is written out, it is stamped with a success marker at the
// end of the file. If this success marker is not present, then the power loss
// occurred durring the wal file creation and the intent of the query cannot be
// determined. If this is the case, the log is discarded and the represented
// transaction does not happen.
//
// Actions are only replayed if the WAL file is valid, which means it can be
// opened, read, contains the success marker, and contains an expected number of
// bytes. Otherwise, the file is deleted.
//
// WAL files are deleted after transactions are successfully completed, as WAL
// files are only used for recovery.
//
// When a transaction commit is attempted, the transaction manager first appends
// all updated pages to the WAL struct. The stored lsit of pages are logged to
// the WAL file.
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

// WriteLog loops through WAL.pages, serializing them, and then writing them out
// to path.
//
// When serializing, a success marker is placed at the end of the byte array to
// be written out. If the bytes could not be successfully written out, there
// should be no success marker present in the final WAL file.
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

	return walFile.Close()
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

// RecoverFromLog inspects a write-ahead-log file.
// If the file appears to be valid, the actions are replayed and any logged
// nodes are re-committed.
//
// Will not perform a recovery if WAL file cannot be opened or read, appears to
// contain invalid contents, or does not contain a success marker indicating it
// was atomically written.
//
// Returns any errors that may have been encountered by std library functions,
// no custom errors indicating the failed step. If power-loss happened and the
// intended action of the user cannot be fully determined, the actions are
// simply discarded, we do not necessarily care why.
func RecoverFromLog(path string, pager *Pager) error {
	logFile, err := os.Open(path)
	if err != nil {
		return err
	}

	info, err := logFile.Stat()
	if err != nil {
		return err
	}

	size := info.Size()
	if !verifyWalFileSize(size) { // File predicted to contain pages?
		return closeAndRemove(logFile)
	}

	buf := make([]byte, 4)
	_, err = logFile.ReadAt(buf, size-4)
	if err != nil {
		return err
	}

	if !bytes.Equal(buf, globals.WalSuccessMarker) { // Success marker present?
		return closeAndRemove(logFile)
	}

	contents := make([]byte, size-4)
	_, err = logFile.ReadAt(contents, 0)
	if err != nil {
		return err
	}

	if err := replayLog(contents, pager); err != nil {
		return closeAndRemove(logFile)
	}

	return closeAndRemove(logFile)
}

// Closes the opened *os.File and returns any errors from os.Remove().
func closeAndRemove(f *os.File) error {
	if err := f.Close(); err != nil {
		return err
	}
	return os.Remove(f.Name())
}

// verifyWalFileSize returns whether the file contents can be cleanly divided
// into globals.PageSize sections of bytes.
// If (size - successMarkerSize) % globals.PageSize == 0 -> file is good.
func verifyWalFileSize(size int64) bool {
	sizeAllPages := size - 4 // exclude success marker

	if sizeAllPages%int64(globals.PageSize) == 0 {
		// Contents can be sliced into pages.
		return true
	}

	// Contents are not aligned with page size.
	return false
}

// replayLog attempts to write out the changed nodes from a valid WAL file.
// This process involves getting the page numbers from the first page of the WAL
// file, and then getting all subsquent page contents in globals.PageSize blocks
// and recretaing the pages.
//
// Recreated pages are written out by pager, and if any error is encountered in
// the pager writing process, the loop is cut short and the error is returned.
func replayLog(log []byte, pager *Pager) error {
	pageNums := make([]byte, globals.PageSize)
	bytesRead := copy(pageNums, log)
	log = log[bytesRead:] // remove pageNum array page

	numItems := len(log) / globals.PageSize

	for range numItems {
		pgNumBytes := make([]byte, globals.PageNumSize)
		bytesRead := copy(pgNumBytes, pageNums)
		pageNums = pageNums[bytesRead:] // remove read page number
		pn := pageNum(binary.LittleEndian.Uint64(pgNumBytes))

		pageContents := make([]byte, globals.PageSize)
		bytesRead = copy(pageContents, log)
		log = log[bytesRead:] // remove read page contents

		pg := NewEmptyPage(pn)
		pg.contents = pageContents
		if err := pager.WritePage(pg); err != nil {
			return err
		}
	}

	return nil
}
