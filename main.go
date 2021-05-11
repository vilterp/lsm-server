package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"path"

	"github.com/vilterp/kv-server/server"
)

type kvServer struct {
	lsm *server.LSM

	mux http.ServeMux
}

func NewKVServer(lsm *server.LSM) *kvServer {
	s := &kvServer{
		lsm: lsm,
		mux: http.ServeMux{},
	}
	s.mux.HandleFunc("/set", s.handleSet)
	s.mux.HandleFunc("/get", s.handleGet)
	return s
}

func (k *kvServer) handleSet(w http.ResponseWriter, req *http.Request) {
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
		if err := k.lsm.Put([]byte(key), []byte(value[0])); err != nil {
			http.Error(w, "internal server error", http.StatusInternalServerError)
			log.Println("error handling set:", err)
			return
		}
	}
}

func (k *kvServer) handleGet(w http.ResponseWriter, req *http.Request) {
	keys := req.URL.Query()["key"]
	if len(keys) != 1 {
		http.Error(w, fmt.Sprintf("should pass exactly one keys"), http.StatusBadRequest)
		return
	}

	value, ok, err := k.lsm.Get([]byte(keys[0]))
	if err != nil {
		http.Error(w, "internal server error", http.StatusInternalServerError)
		log.Println("error handling get:", err)
		return
	}
	if !ok {
		http.NotFound(w, req)
		return
	}

	if _, err := fmt.Fprintln(w, value); err != nil {
		log.Println("error writing response:", err)
	}
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
	dataDir, found := os.LookupEnv("DATA_DIR")
	if !found {
		dataDir = "data"
	}

	// construct server
	walFile, err := server.NewKVFile(path.Join(dataDir, "wal.kv.gob"))
	if err != nil {
		log.Fatal("error opening wal: ", err)
	}
	lsm, err := server.NewLSM(walFile, dataDir)
	if err != nil {
		log.Fatal("error creating LSM: ", err)
	}
	serv := NewKVServer(lsm)

	log.Printf("listening on %s:%s", host, port)
	if err := http.ListenAndServe(fmt.Sprintf("%s:%s", host, port), serv); err != nil {
		log.Fatal("error listening:", err)
	}
}
