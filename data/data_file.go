package data

import (
	"fmt"
	"kv-project/fio"
	"path/filepath"
)

const FileNameSuffix = ".data"

type File struct {
	FileID      uint32
	WriteOffset int64
	IOManager   fio.IOManager
}

// OpenDataFile open a new data file
func OpenDataFile(dirPath string, fileID uint32) (*File, error) {
	fileName := filepath.Join(dirPath, fmt.Sprintf("%09d", fileID)+FileNameSuffix)
	ioManager, err := fio.NewFileIOManager(fileName)
	if err != nil {
		return nil, err
	}
	return &File{FileID: fileID, WriteOffset: 0, IOManager: ioManager}, nil
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
