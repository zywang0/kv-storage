package data

import (
	"encoding/binary"
	"hash/crc32"
)

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
	header := make([]byte, maxLogRecordHeaderSize)
	//store type
	header[4] = record.Type

	//store key/value size
	var index = 5
	index += binary.PutVarint(header[index:], int64(len(record.Key)))
	index += binary.PutVarint(header[index:], int64(len(record.Value)))

	var encRecordSize = index + len(record.Key) + len(record.Value)
	encBytes := make([]byte, encRecordSize)

	//store header
	copy(encBytes[:index], header[:index])

	//store key/value
	copy(encBytes[index:], record.Key)
	copy(encBytes[index+len(record.Key):], record.Value)

	//calculate and store crc value
	crc := crc32.ChecksumIEEE(encBytes[4:])
	binary.LittleEndian.PutUint32(encBytes[:4], crc)

	return encBytes, int64(encRecordSize)
}

func DecodeLogRecord(buff []byte) (*LogRecordHeader, int64) {
	if len(buff) <= 4 {
		return nil, 0
	}

	header := &LogRecordHeader{
		crc:        binary.LittleEndian.Uint32(buff[:4]),
		recordType: buff[4],
	}

	var index = 5
	keySize, size := binary.Varint(buff[index:])
	header.keySize = uint32(keySize)
	index += size

	valueSize, size := binary.Varint(buff[index:])
	header.valueSize = uint32(valueSize)
	index += size

	return header, int64(index)
}

func GetRecordCRC(record *LogRecord, header []byte) uint32 {
	// TODO
	return 0
}
