package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
)

type kvServer struct {
	contents map[string]string
	mutex sync.Mutex

	mux http.ServeMux
}

func NewKVServer() *kvServer {
	s := &kvServer{
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
	for _, value := range req.URL.Query() {
		if len(value) > 1 {
			http.Error(w, fmt.Sprintf("multiple values for key"), http.StatusBadRequest)
			return
		}
	}

	query := req.URL.Query()
	for key, value := range query {
		k.contents[key] = value[0]
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

	_, err := fmt.Fprintln(w, k.contents[keys[0]])
	if err != nil {
		log.Println("error writing response:", err)
	}
}

func (k *kvServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	log.Printf("%s %s", req.Method, req.RequestURI)
	k.mux.ServeHTTP(w, req)
}

func main() {
	server := NewKVServer()

	host, found := os.LookupEnv("HOST")
	if !found {
		host = "localhost"
	}
	port, found := os.LookupEnv("PORT")
	if !found {
		port = "9999"
	}

	log.Printf("listening on %s:%s", host, port)
	if err := http.ListenAndServe(fmt.Sprintf("%s:%s", host, port), server); err != nil {
		log.Fatal("error listening:", err)
	}
}
