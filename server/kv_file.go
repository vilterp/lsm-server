package server

import (
	"encoding/gob"
	"io"
	"os"
)

type KVPair struct {
	Key   []byte
	Value []byte
}

// === file ===

type KVFile struct {
	encoder *gob.Encoder
	file    *os.File
}

func NewKVFile(path string) (*KVFile, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	return &KVFile{
		file:    file,
		encoder: gob.NewEncoder(file),
	}, nil
}

func (kvf *KVFile) AppendKVPair(key []byte, value []byte) error {
	if _, err := kvf.file.Seek(0, 2); err != nil {
		return nil
	}

	if err := kvf.encoder.Encode(KVPair{
		Key:   key,
		Value: value,
	}); err != nil {
		return err
	}
	return nil
}

// === reader ===

type KVFileReader struct {
	decoder *gob.Decoder
}

func (kvf *KVFile) GetReader() (*KVFileReader, error) {
	file, err := os.Open(kvf.file.Name())
	if err != nil {
		return nil, err
	}
	return &KVFileReader{
		decoder: gob.NewDecoder(file),
	}, nil
}

func (r *KVFileReader) Next() (*KVPair, error) {
	var out KVPair
	if err := r.decoder.Decode(&out); err != nil {
		if err == io.EOF {
			return nil, nil
		}
		return nil, err
	}
	return &out, nil
}
