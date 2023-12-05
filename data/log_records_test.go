package data

import (
	"github.com/stretchr/testify/assert"
	"hash/crc32"
	"testing"
)

func TestEncodeLogRecord(t *testing.T) {
	// normal case
	rec1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  LogRecordNormal,
	}
	res1, n1 := EncodeLogRecord(rec1)
	t.Log("header1:", res1[:n1-int64(len(rec1.Key)+len(rec1.Value))])
	t.Log("crc1 value:", crc32.ChecksumIEEE(res1[4:]))
	assert.NotNil(t, res1)
	assert.Greater(t, n1, int64(5))

	// value is empty
	rec2 := &LogRecord{
		Key:  []byte("name"),
		Type: LogRecordNormal,
	}
	res2, n2 := EncodeLogRecord(rec2)
	t.Log("header2:", res2[:n2-int64(len(rec2.Key)+len(rec2.Value))])
	t.Log("crc2 value:", crc32.ChecksumIEEE(res2[4:]))
	assert.NotNil(t, res2)
	assert.Greater(t, n2, int64(5))

	// type is Deleted
	rec3 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  LogRecordDeleted,
	}
	res3, n3 := EncodeLogRecord(rec3)
	t.Log("header3:", res3[:n1-int64(len(rec3.Key)+len(rec3.Value))])
	t.Log("crc3 value:", crc32.ChecksumIEEE(res3[4:]))
	assert.NotNil(t, res3)
	assert.Greater(t, n3, int64(5))
}

func TestDecodeLogRecordHeader(t *testing.T) {
	// normal case
	headerBuf1 := []byte{104, 82, 240, 150, 0, 8, 20}
	h1, size1 := DecodeLogRecordHeader(headerBuf1)
	assert.NotNil(t, h1)
	assert.Equal(t, int64(7), size1)
	assert.Equal(t, uint32(2532332136), h1.crc)
	assert.Equal(t, LogRecordNormal, h1.recordType)
	assert.Equal(t, uint32(4), h1.keySize)
	assert.Equal(t, uint32(10), h1.valueSize)

	// value is empty
	headerBuf2 := []byte{9, 252, 88, 14, 0, 8, 0}
	h2, size2 := DecodeLogRecordHeader(headerBuf2)
	assert.NotNil(t, h2)
	assert.Equal(t, int64(7), size2)
	assert.Equal(t, uint32(240712713), h2.crc)
	assert.Equal(t, LogRecordNormal, h2.recordType)
	assert.Equal(t, uint32(4), h2.keySize)
	assert.Equal(t, uint32(0), h2.valueSize)

	// type is Deleted
	headerBuf3 := []byte{43, 153, 86, 17, 1, 8, 20}
	h3, size3 := DecodeLogRecordHeader(headerBuf3)
	assert.NotNil(t, h3)
	assert.Equal(t, int64(7), size3)
	assert.Equal(t, uint32(290887979), h3.crc)
	assert.Equal(t, LogRecordDeleted, h3.recordType)
	assert.Equal(t, uint32(4), h3.keySize)
	assert.Equal(t, uint32(10), h3.valueSize)
}

func TestGetRecordCRC(t *testing.T) {
	// normal case
	rec1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  LogRecordNormal,
	}
	headerBuf1 := []byte{104, 82, 240, 150, 0, 8, 20}
	crc1 := GetRecordCRC(rec1, headerBuf1[crc32.Size:])
	assert.Equal(t, uint32(2532332136), crc1)

	// type is Deleted
	rec2 := &LogRecord{
		Key:  []byte("name"),
		Type: LogRecordNormal,
	}
	headerBuf2 := []byte{9, 252, 88, 14, 0, 8, 0}
	crc2 := GetRecordCRC(rec2, headerBuf2[crc32.Size:])
	assert.Equal(t, uint32(240712713), crc2)

	// type is Deleted
	rec3 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  LogRecordDeleted,
	}
	headerBuf3 := []byte{43, 153, 86, 17, 1, 8, 20}
	crc3 := GetRecordCRC(rec3, headerBuf3[crc32.Size:])
	assert.Equal(t, uint32(290887979), crc3)
}
