package main

import (
	"fmt"
	kv "kv-project"
)

func main() {
	options := kv.DefaultOptions
	options.DirPath = "/tmp/bitcaskDemo"
	db, err := kv.Start(options)
	if err != nil {
		panic(err)
	}

	defer func(db *kv.DB) {
		_ = db.Close()
	}(db)

	key := []byte("db")
	value := []byte("bitcask")

	err = db.Put(key, value)
	if err != nil {
		panic(err)
	}

	val, err := db.Get(key)
	if err != nil {
		panic(err)
	}
	fmt.Println("val = ", string(val))

	err = db.Delete(key)
	if err != nil {
		panic(err)
	}
	fmt.Println("Deleted key: ", string(key))
}
