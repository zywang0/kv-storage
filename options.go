package kv_project

import "os"

type Options struct {
	//data directory
	DirPath string

	//the size of data file
	DataFileSize int64

	//Check if data is persistent on every write
	SyncWrites bool

	//index type
	IndexType IndexerType
}

type IndexerType = int8

const (
	Btree IndexerType = iota + 1
	ART
)

var DefaultOptions = Options{
	DirPath:      os.TempDir(),
	DataFileSize: 256 * 1024 * 1024,
	SyncWrites:   false,
	IndexType:    Btree,
}

type IteratorOptions struct {
	Prefix  []byte
	Reverse bool
}

var DefaultIteratorOptions = IteratorOptions{
	// Prefix specifies the prefix value for keys to iterate over. Default is empty.
	Prefix: nil,
	// Reverse indicates whether to iterate in reverse order. Default is false for forward iteration.
	Reverse: false,
}
