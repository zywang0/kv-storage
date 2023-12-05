package data

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io"
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

// ReadLogRecord read record at offset and return record and its size
func (f *File) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	fileSize, err := f.IOManager.Size()
	if err != nil {
		return nil, 0, err
	}

	//header is read according to the max Header Size
	var logRecordHeaderSize int64 = maxLogRecordHeaderSize

	//if exceed file size, we will handle EOF and only read to the end of the file
	if offset+maxLogRecordHeaderSize > fileSize {
		logRecordHeaderSize = fileSize - offset
	}

	headerBuff, err := f.ReadNBytes(logRecordHeaderSize, offset)
	if err != nil {
		return nil, 0, err
	}

	//get final real header and its size
	header, headerSize := DecodeLogRecordHeader(headerBuff)

	//edge case
	if header == nil {
		return nil, 0, io.EOF
	}
	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF
	}

	keySize := int64(header.keySize)
	valueSize := int64(header.valueSize)

	var recordSize = headerSize + keySize + valueSize

	record := &LogRecord{Type: header.recordType}

	//read real key and value
	if keySize > 0 || valueSize > 0 {
		kvBuff, err := f.ReadNBytes(keySize+valueSize, offset+headerSize)
		if err != nil {
			return nil, 0, err
		}
		record.Key = kvBuff[:keySize]
		record.Value = kvBuff[keySize:]
	}

	recordCRC := GetRecordCRC(record, headerBuff[crc32.Size:headerSize])
	if recordCRC != header.crc {
		return nil, 0, errors.New("invalid crc value")
	}

	return record, recordSize, nil
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

// ReadNBytes convert size from int64 to []byte so that read n size bytes at offset
func (f *File) ReadNBytes(size int64, offset int64) ([]byte, error) {
	bytes := make([]byte, size)
	_, err := f.IOManager.Read(bytes, offset)
	if err != nil {
		return nil, err
	}
	return bytes, err
}
