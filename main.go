package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"path"

	"github.com/vilterp/kv-server/server"
	"github.com/vilterp/kv-server/storage"
)

func main() {
	// get env vars
	host, found := os.LookupEnv("HOST")
	if !found {
		host = "localhost"
	}
	port, found := os.LookupEnv("PORT")
	if !found {
		port = "9999"
	}
	dataDir, found := os.LookupEnv("DATA_DIR")
	if !found {
		dataDir = "data"
	}

	// construct server
	walFile, err := storage.NewKVFile(path.Join(dataDir, "wal.kv"))
	if err != nil {
		log.Fatal("error opening wal: ", err)
	}
	lsm, err := storage.NewLSM(walFile, dataDir)
	if err != nil {
		log.Fatal("error creating LSM: ", err)
	}
	serv := server.NewKVServer(lsm)

	log.Printf("listening on %s:%s", host, port)
	if err := http.ListenAndServe(fmt.Sprintf("%s:%s", host, port), serv); err != nil {
		log.Fatal("error listening:", err)
	}
}
