package kv_project

import (
	"errors"
	"io"
	"kv-project/data"
	"kv-project/index"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type DB struct {
	options      Options
	mu           *sync.RWMutex
	activeFile   *data.File            // can be used for append writing
	inactiveFile map[uint32]*data.File // only can be used to read
	index        index.Indexer
	fileIds      []int
}

// Start bitcask database storage instance startup process
func Start(options Options) (*DB, error) {
	//verify user options
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	//check if data file exists
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		//create this file if it doesn't exist
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	//initialize database instance
	db := &DB{
		options:      options,
		mu:           new(sync.RWMutex),
		inactiveFile: make(map[uint32]*data.File),
		index:        index.NewIndexer(options.IndexType),
	}

	//load the data file
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	//load index from data file
	if err := db.loadIndexFromDataFiles(); err != nil {
		return nil, err
	}
	return db, nil
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
	record, _, err := dataFile.ReadLogRecord(recordPos.Offset)
	if err != nil {
		return nil, err
	}

	//check if the record deleted
	if record.Type == data.LogRecordDeleted {
		return nil, ErrDataFileDeleted
	}

	return record.Value, nil

}

func (db *DB) Delete(key []byte) error {
	//check if the key is valid
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	//get value through the key of index
	recordPos := db.index.Get(key)
	//check if key exists
	//data file will inflate if the user keeps deleting a non-existent key
	if recordPos == nil {
		return ErrKeyNotFound
	}

	record := &data.LogRecord{Key: key, Type: data.LogRecordDeleted}
	if _, err := db.appendLogRecord(record); err != nil {
		return err
	}

	//update memory index
	deleted := db.index.Delete(key)
	if !deleted {
		return ErrIndexUpdateFailed
	}

	return nil
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

// loadDataFiles load data files from the disk
func (db *DB) loadDataFiles() error {
	dir, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}
	var fileIds []int
	//iterate over each file in directory to find the file ending in `.data`
	for _, entry := range dir {
		if strings.HasSuffix(entry.Name(), data.FileNameSuffix) {
			splitNames := strings.Split(entry.Name(), ".")
			fileId, err := strconv.Atoi(splitNames[0])
			if err != nil {
				return ErrDataDirCorrupted
			}
			fileIds = append(fileIds, fileId)
		}
	}
	sort.Ints(fileIds)
	db.fileIds = fileIds

	//iterate over each file id
	for i, fid := range fileIds {
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fid))
		if err != nil {
			return err
		}
		if i == len(fileIds)-1 {
			db.activeFile = dataFile
		} else {
			db.inactiveFile[uint32(fid)] = dataFile
		}
	}
	return nil
}

// loadIndexFromDataFiles iterate over each fileID and update index
func (db *DB) loadIndexFromDataFiles() error {
	//no files in the database
	if len(db.fileIds) == 0 {
		return nil
	}

	//iterate over each fileID
	for i, fid := range db.fileIds {
		var fileId = uint32(fid)
		var dataFile *data.File
		if fileId == db.activeFile.FileID {
			dataFile = db.activeFile
		} else {
			dataFile = db.inactiveFile[fileId]
		}

		var offset int64 = 0
		//get record through offset
		for {
			record, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				// reaching the end of record
				if err == io.EOF {
					break
				}
				return err
			}

			logRecordPos := &data.LogRecordPos{
				Fid:    fileId,
				Offset: offset,
			}
			var status bool
			if record.Type == data.LogRecordDeleted {
				status = db.index.Delete(record.Key)
			} else {
				status = db.index.Put(record.Key, logRecordPos)
			}
			if !status {
				return ErrIndexUpdateFailed
			}

			//update offset so that it can be read from the next new position
			offset += size
		}

		//update write offset of this active file when reaching the end of the file
		if i == len(db.fileIds)-1 {
			db.activeFile.WriteOffset = offset
		}
	}
	return nil
}

func checkOptions(options Options) error {
	if options.DirPath == "" {
		return errors.New("directory path cannot be empty")
	}
	if options.DataFileSize <= 0 {
		return errors.New("data file size should be greater than 0")
	}
	return nil
}
