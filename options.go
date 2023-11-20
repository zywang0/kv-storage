package kv_project

type Options struct {
	//data directory
	DirPath string

	//the size of data file
	DataFileSize int64

	//Check if data is persistent on every write
	SyncWrites bool
}
