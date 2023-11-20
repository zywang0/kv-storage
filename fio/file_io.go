package fio

import "os"

type FileIO struct {
	fileDescription *os.File
}

func NewFileIOManager(fileName string) (*FileIO, error) {
	file, err := os.OpenFile(
		fileName,
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		DataFilePerm,
	)

	if err != nil {
		return nil, err
	}
	return &FileIO{fileDescription: file}, nil
}

func (fio *FileIO) Read(b []byte, off int64) (int, error) {
	return fio.fileDescription.ReadAt(b, off)
}

func (fio *FileIO) Write(b []byte) (int, error) {
	return fio.fileDescription.Write(b)
}

func (fio *FileIO) Sync() error {
	return fio.fileDescription.Sync()
}

func (fio *FileIO) Close() error {
	return fio.fileDescription.Close()
}

func (fio *FileIO) Size() (int64, error) {
	fileInfo, err := fio.fileDescription.Stat()
	if err != nil {
		return 0, err
	}
	return fileInfo.Size(), nil
}
