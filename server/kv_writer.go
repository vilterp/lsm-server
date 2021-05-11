package server

import (
	"encoding/binary"
	"os"
)

type KVWriter struct {
	file *os.File
}

func NewKVWriter(path string) (*KVWriter, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	return &KVWriter{
		file: file,
	}, nil
}

func (w *KVWriter) AppendKVPair(key []byte, value []byte) error {
	// TODO: update index?
	if err := binary.Write(w.file, binary.LittleEndian, int32(len(key))); err != nil {
		return err
	}
	if _, err := w.file.Write(key); err != nil {
		return err
	}
	if err := binary.Write(w.file, binary.LittleEndian, int32(len(value))); err != nil {
		return err
	}
	if _, err := w.file.Write(value); err != nil {
		return err
	}
	return nil
}
