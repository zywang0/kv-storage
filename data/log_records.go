package data

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

// EncodeLogRecord return byte array and its size
func EncodeLogRecord(record *LogRecord) ([]byte, int64) {
	return nil, 0
}
