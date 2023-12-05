package main

import (
	"fmt"
	kv "kv-project"
)

func main() {
	options := kv.DefaultOptions
	db, err := kv.Start(options)
	if err != nil {
		panic(err)
	}

	err = db.Put([]byte("db"), []byte("bitcask"))
	if err != nil {
		panic(err)
	}

	val, err := db.Get([]byte("db"))
	if err != nil {
		panic(err)
	}
	fmt.Println("val = ", string(val))

	err = db.Delete([]byte("db"))
	if err != nil {
		panic(err)
	}
	fmt.Println("Deleted key: ", string([]byte("db")))
}
