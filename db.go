package kv_project

import (
	"kv-project/data"
	"kv-project/index"
	"sync"
)

type DB struct {
	options      Options
	mu           *sync.RWMutex
	activeFile   *data.File            // can be used to write
	inactiveFile map[uint32]*data.File // only can be used to read
	index        index.Indexer
}

// Put write/update k-v data
func (db *DB) Put(key []byte, value []byte) error {
	//check if the key is valid
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	record := &data.LogRecord{Key: key, Value: value, Type: data.LogRecordNormal}

	//Append to currently active file
	recordPos, err := db.appendLogRecord(record)
	if err != nil {
		return err
	}

	//Update memory index
	if status := db.index.Put(key, recordPos); status != true {
		return ErrIndexUpdateFailed
	}
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	//check if the key is valid
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	//get value through the key of index
	recordPos := db.index.Get(key)
	//check if key exists
	if recordPos == nil {
		return nil, ErrKeyNotFound
	}

	var dataFile *data.File
	//check if the record is in the active file
	if recordPos.Fid == db.activeFile.FileID {
		dataFile = db.activeFile
	} else {
		dataFile = db.inactiveFile[recordPos.Fid]
	}

	//check if dataFile exists
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	//read data based on offset
	record, err := dataFile.ReadLogRecord(recordPos.Offset)
	if err != nil {
		return nil, err
	}

	//check if the record deleted
	if record.Type == data.LogRecordDeleted {
		return nil, ErrDataFileDeleted
	}

	return record.Value, nil

}

// appendLogRecord write data into active file, return the position of this write
func (db *DB) appendLogRecord(record *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	//Step 1: Initialize active file
	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	//Step 2: Open the active file
	//Encode record
	encRecord, size := data.EncodeLogRecord(record)
	//If the written data size has reached the active file threshold, close active file and open a new file
	if db.activeFile.WriteOffset+size > db.options.DataFileSize {

		//Persistent existed data files
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		//change activeFile to inactive
		db.inactiveFile[db.activeFile.FileID] = db.activeFile

		//open a new data file
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	//Step 3: Write encoded records into the active file
	//Record the starting position of this write
	writeOff := db.activeFile.WriteOffset
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}

	//Persistence is determined by configuration options
	if db.options.SyncWrites {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}

	//Construct and return memory index position
	pos := &data.LogRecordPos{Fid: db.activeFile.FileID, Offset: writeOff, Size: uint32(size)}
	return pos, nil
}

func (db *DB) setActiveDataFile() error {
	var initialFileID uint32 = 0
	if db.activeFile != nil {
		initialFileID = db.activeFile.FileID + 1
	}

	openDataFile, err := data.OpenDataFile(db.options.DirPath, initialFileID)
	if err != nil {
		return err
	}
	db.activeFile = openDataFile
	return nil
}
