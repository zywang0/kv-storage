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
	return nil, nil
}

func (f *File) Sync() error {
	return nil
}

func (f *File) Write(buf []byte) error {
	return nil
}

func (f *File) ReadLogRecord(offset int64) (*LogRecord, error) {
	return nil, nil
}
