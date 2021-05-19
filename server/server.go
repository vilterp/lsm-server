package server

import (
	"fmt"
	"log"
	"net/http"

	"github.com/vilterp/lsm-server/storage"
)

type kvServer struct {
	lsm *storage.LSM

	mux http.ServeMux
}

func NewKVServer(lsm *storage.LSM) *kvServer {
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
		http.Error(w, "expecting exactly one key", http.StatusBadRequest)
		return
	}
	for _, value := range req.URL.Query() {
		if len(value) > 1 {
			http.Error(w, "multiple values for key", http.StatusBadRequest)
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

	w.WriteHeader(http.StatusCreated)
}

func (k *kvServer) handleGet(w http.ResponseWriter, req *http.Request) {
	keys := req.URL.Query()["key"]
	if len(keys) != 1 {
		http.Error(w, "should pass exactly one keys", http.StatusBadRequest)
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

	if _, err := fmt.Fprintln(w, string(value)); err != nil {
		log.Println("error writing response:", err)
	}
}

func (k *kvServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	log.Printf("%s %s", req.Method, req.RequestURI)
	k.mux.ServeHTTP(w, req)
}
