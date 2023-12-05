package kv_project

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
	DirPath:      "/tmp/bitcaskDemo",
	DataFileSize: 256 * 1024 * 1024,
	SyncWrites:   false,
	IndexType:    Btree,
}
