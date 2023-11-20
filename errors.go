package kv_project

import "errors"

var (
	ErrKeyIsEmpty = errors.New("empty Key")

	ErrIndexUpdateFailed = errors.New("index update failed")

	ErrKeyNotFound = errors.New("key is not found")

	ErrDataFileNotFound = errors.New("data file is not found")

	ErrDataFileDeleted = errors.New("data file is deleted")
)
