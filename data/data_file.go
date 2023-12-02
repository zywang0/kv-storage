package data

import (
	"kv-project/fio"
)

const FileNameSuffix = ".data"

type File struct {
	FileID      uint32
	WriteOffset int64
	IOManager   fio.IOManager
}

func OpenDataFile(dirPath string, fileID uint32) (*File, error) {
	// TODO
	return nil, nil
}

func (f *File) Sync() error {
	// TODO
	return nil
}

func (f *File) Write(buf []byte) error {
	// TODO
	return nil
}

func (f *File) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	// TODO
	return nil, 0, nil
}
