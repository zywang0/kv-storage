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
	return f.IOManager.Sync()
}

func (f *File) Write(buf []byte) error {
	writeBytes, err := f.IOManager.Write(buf)
	if err != nil {
		return err
	}
	f.WriteOffset += int64(writeBytes)
	return nil
}

func (f *File) Close() error {
	return f.IOManager.Close()
}
