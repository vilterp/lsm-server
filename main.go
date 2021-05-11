package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"

	"github.com/vilterp/kv-server/server"
)

type kvServer struct {
	contents map[string]string
	mutex sync.Mutex

	wal *server.KVFile

	mux http.ServeMux
}

func NewKVServer(walWriter *server.KVFile) *kvServer {
	s := &kvServer{
		wal: walWriter,
		contents: map[string]string{},
		mux: http.ServeMux{},
	}
	s.mux.HandleFunc("/set", s.handleSet)
	s.mux.HandleFunc("/get", s.handleGet)
	return s
}

func (k *kvServer) handleSet(w http.ResponseWriter, req *http.Request) {
	k.mutex.Lock()
	defer k.mutex.Unlock()

	// validate the input: reject if more than one value for a key
	if len(req.URL.Query()) != 1 {
		http.Error(w, fmt.Sprintf("expecting exactly one key"), http.StatusBadRequest)
		return
	}
	for _, value := range req.URL.Query() {
		if len(value) > 1 {
			http.Error(w, fmt.Sprintf("multiple values for key"), http.StatusBadRequest)
			return
		}
	}

	query := req.URL.Query()
	for key, value := range query {
		k.contents[key] = value[0]
		if err := k.wal.AppendKVPair([]byte(key), []byte(value[0])); err != nil {
			http.Error(w, fmt.Sprintf("error writing key: %s", err), http.StatusInternalServerError)
			return
		}
	}
}

func (k *kvServer) handleGet(w http.ResponseWriter, req *http.Request) {
	k.mutex.Lock()
	defer k.mutex.Unlock()

	keys := req.URL.Query()["key"]
	if len(keys) != 1 {
		http.Error(w, fmt.Sprintf("should pass exactly one keys"), http.StatusBadRequest)
		return
	}

	value, ok := k.contents[keys[0]]
	if !ok {
		http.NotFound(w, req)
		return
	}

	_, err := fmt.Fprintln(w, value)
	if err != nil {
		log.Println("error writing response:", err)
	}
}

func (k *kvServer) loadFromWAL() (int, error) {
	k.mutex.Lock()
	defer k.mutex.Unlock()

	read := 0

	reader := k.wal.GetReader()
	for {
		kvPair, err := reader.Next()
		if err != nil {
			return 0, err
		}
		if kvPair == nil {
			break
		}

		// TODO: don't stringify
		k.contents[string(kvPair.Key)] = string(kvPair.Value)
		read++
	}
	return read, nil
}

func (k *kvServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	log.Printf("%s %s", req.Method, req.RequestURI)
	k.mux.ServeHTTP(w, req)
}

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
	file, found := os.LookupEnv("FILE")
	if !found {
		file = "wal.kv"
	}

	// construct server
	walFile, err := server.NewKVFile(file)
	if err != nil {
		log.Fatal("error opening wal:", err)
	}
	serv := NewKVServer(walFile)
	read, err := serv.loadFromWAL()
	if err != nil {
		log.Fatal("error reading log:", err)
	}
	log.Printf("read %d keys on startup", read)

	log.Printf("listening on %s:%s", host, port)
	if err := http.ListenAndServe(fmt.Sprintf("%s:%s", host, port), serv); err != nil {
		log.Fatal("error listening:", err)
	}
}
