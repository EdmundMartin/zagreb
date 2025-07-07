package main

import (
	"log"
	"zagreb/pkg/api"
	"zagreb/pkg/storage/bbolt"
)

func main() {
	dbPath := "./my.db"
	bboltStorage, err := bbolt.NewBBoltStorage(dbPath)
	if err != nil {
		log.Fatalf("failed to create bbolt storage: %v", err)
	}

	server := api.NewServer(bboltStorage)
	server.Run(":8000") // Listen on port 8000
}
