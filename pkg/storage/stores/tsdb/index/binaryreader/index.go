package binaryreader

import (
	"hash/crc32"
	"unsafe"
)

// The table gets initialized with sync.Once but may still cause a race
// with any other use of the crc32 package anywhere. Thus we initialize it
// before.
var castagnoliTable *crc32.Table

func init() {
	castagnoliTable = crc32.MakeTable(crc32.Castagnoli)
}

// StringIter iterates over a sorted list of strings.
type StringIter interface {
	// Next advances the iterator and returns true if another value was found.
	Next() bool

	// At returns the value at the current iterator position.
	At() string

	// Err returns the last error of the iterator.
	Err() error
}

func yoloString(b []byte) string {
	return *((*string)(unsafe.Pointer(&b)))
}
