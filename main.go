package main

import (
	"errors"
	"fmt"
	"github.com/gorilla/mux"
	"io"
	"kvalStore/logger"
	"log"
	"net/http"
	"sync"
)

const KEY = "key"

var ErrNoSuchKey = errors.New("no such key")
var store = struct {
	sync.RWMutex
	m map[string]string
}{m: make(map[string]string)}
var l logger.TransactionLogger

func main() {
	r := mux.NewRouter()
	err := initializeTransactionLogger()
	if err != nil {
		panic(err)
		return
	}

	r.HandleFunc("/v1/{key}", PutHandler).Methods(http.MethodPut)
	r.HandleFunc("/v1/{key}", GetHandler).Methods(http.MethodGet)
	r.HandleFunc("/v1/{key}", DeleteHandler).Methods(http.MethodDelete)

	fmt.Println("initializing server in port :8080")
	log.Fatal(http.ListenAndServe(":8080", r))
}

func Put(key, value string) error {
	store.Lock()
	store.m[key] = value
	store.Unlock()
	return nil
}

func Get(key string) (string, error) {
	store.RLock()
	value, ok := store.m[key]
	store.RUnlock()

	if !ok {
		return "", ErrNoSuchKey
	}

	return value, nil
}

func Delete(key string) error {
	store.Lock()
	delete(store.m, key)
	store.Unlock()

	return nil
}

func PutHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars[KEY]

	value, err := io.ReadAll(r.Body)
	defer r.Body.Close()

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = Put(key, string(value))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	l.WritePut(key, string(value))
	w.WriteHeader(http.StatusCreated)
}

func GetHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars[KEY]

	value, err := Get(key)

	if errors.Is(err, ErrNoSuchKey) {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Write([]byte(value))
	return

}

func DeleteHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars[KEY]

	err := Delete(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	l.WriteDelete(key)
	w.WriteHeader(http.StatusOK)
}

func initializeTransactionLogger() error {
	var err error //avoid initializing new error to use 'logger' var in file scope

	l, err = logger.NewFileTransactionLogger("transaction.log")
	if err != nil {
		return fmt.Errorf("failed to create event logger: %w", err)
	}

	events, errorsChan := l.ReadEvents()
	e, ok := logger.Event{}, true

	for ok && err == nil {
		select {
		case err, ok = <-errorsChan:
		case e, ok = <-events:
			switch e.EventType {
			case logger.EventDelete:
				err = Delete(e.Key)
			case logger.EventPut:
				err = Put(e.Key, e.Value)
			}
		}
	}

	l.Run()

	return err
}
