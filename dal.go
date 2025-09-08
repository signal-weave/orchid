package main

import (
	"fmt"
	"os"
)

type pgnum uint64
type page struct {
	num  pgnum
	data []byte
}

type dataAccessLayer struct {
	file     *os.File
	pageSize int
	*freelist
}

func newDal(path string, pageSize int) (*dataAccessLayer, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	dal := &dataAccessLayer{
		file,
		pageSize,
		newFreelist(),
	}

	return dal, nil
}

func (d *dataAccessLayer) close() error {
	if d.file != nil {
		err := d.file.Close()
		if err != nil {
			return fmt.Errorf("could not close file: %s", err)
		}
		d.file = nil
	}

	return nil
}

func (d *dataAccessLayer) allocateEmptyPage() *page {
	return &page{
		data: make([]byte, d.pageSize),
	}
}

func (d *dataAccessLayer) readPage(pageNum pgnum) (*page, error) {
	p := d.allocateEmptyPage()

	offset := int(pageNum) * d.pageSize
	_, err := d.file.ReadAt(p.data, int64(offset))
	if err != nil {
		return nil, err
	}
	return p, err
}

func (d *dataAccessLayer) writePage(p *page) error {
	offset := int64(p.num) * int64(d.pageSize)
	_, err := d.file.WriteAt(p.data, offset)
	return err
}
