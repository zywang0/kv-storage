package data

import "encoding/binary"

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
)

type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// LogRecordPos data memory index: describe the location of data on disk
type LogRecordPos struct {
	//uint32 only represents non-negative numbers
	Fid uint32
	//int64 represents both negative and positive numbers
	Offset int64
	//identifies data size on disk
	Size uint32
}

// Header: crc + type + key_size + value_size
const maxLogRecordHeaderSize = 4 + 1 + binary.MaxVarintLen32*2

type LogRecordHeader struct {
	crc        uint32
	recordType LogRecordType
	keySize    uint32
	valueSize  uint32
}

// EncodeLogRecord return byte array and its size
func EncodeLogRecord(record *LogRecord) ([]byte, int64) {
	// TODO
	return nil, 0
}

func DecodeLogRecord(buff []byte) (*LogRecordHeader, int64) {
	// TODO
	return nil, 0
}

func GetRecordCRC(record *LogRecord, header []byte) uint32 {
	// TODO
	return 0
}
