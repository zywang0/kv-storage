package fio

import (
	"golang.org/x/exp/mmap"
	"os"
)

type MMap struct {
	readerAt *mmap.ReaderAt
}

func (M *MMap) Read(bytes []byte, offset int64) (int, error) {
	return M.readerAt.ReadAt(bytes, offset)
}

func (M *MMap) Write(bytes []byte) (int, error) {
	panic("not implemented")
}

func (M *MMap) Sync() error {
	panic("not implemented")
}

func (M *MMap) Close() error {
	return M.readerAt.Close()
}

func (M *MMap) Size() (int64, error) {
	return int64(M.readerAt.Len()), nil
}

func NewMMapIOManager(fileName string) (*MMap, error) {
	_, err := os.OpenFile(
		fileName,
		os.O_CREATE,
		DataFilePerm,
	)

	if err != nil {
		return nil, err
	}

	readerAt, err := mmap.Open(fileName)
	if err != nil {
		return nil, err
	}
	return &MMap{readerAt: readerAt}, nil
}
