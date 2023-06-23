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

func yoloString(b []byte) string {
	return *((*string)(unsafe.Pointer(&b)))
}
