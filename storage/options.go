package storage

import (
	"orchiddb/globals"
)

type Options struct {
	PageSize int

	MinFillPercent float32 // Percentage required to be filled to not del node.
	MinThreshold   float32 // Bytes required to be filled to not del node.

	MaxFillPercent float32 // Percentage to be filled before node split.
	MaxThreshold   float32 // Bytes to be filled before node split.
}

func NewOptions() *Options {
	o := &Options{
		PageSize: globals.PageSize,

		MinFillPercent: globals.MinFillPercent,
		MinThreshold:   globals.MinFillPercent * float32(globals.PageSize),

		MaxFillPercent: globals.MaxFillPercent,
		MaxThreshold:   globals.MaxFillPercent * float32(globals.PageSize),
	}

	return o
}
