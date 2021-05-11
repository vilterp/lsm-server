package server

import (
	"encoding/binary"
	"io"
	"os"
	"sync"
)

var byteOrder binary.ByteOrder

func init() {
	byteOrder = binary.LittleEndian
}

type KVFile struct {
	lock sync.Mutex

	file *os.File
}

func NewKVFile(path string) (*KVFile, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	return &KVFile{
		file: file,
	}, nil
}

func (kvf *KVFile) AppendKVPair(key []byte, value []byte) error {
	kvf.lock.Lock()
	defer kvf.lock.Unlock()

	if _, err := kvf.file.Seek(0, 2); err != nil {
		return nil
	}

	// TODO: update index?
	if err := binary.Write(kvf.file, byteOrder, uint32(len(key))); err != nil {
		return err
	}
	if _, err := kvf.file.Write(key); err != nil {
		return err
	}
	if err := binary.Write(kvf.file, byteOrder, uint32(len(value))); err != nil {
		return err
	}
	if _, err := kvf.file.Write(value); err != nil {
		return err
	}
	return nil
}

type KVFileReader struct {
	kvFile *KVFile
	fileByteIndex int64
}

func (kvf *KVFile) GetReader() *KVFileReader {
	return &KVFileReader{
		kvFile: kvf,
		fileByteIndex: 0,
	}
}

type KVPair struct {
	Key   []byte
	Value []byte
}

func (r *KVFileReader) Next() (*KVPair, error) {
	r.kvFile.lock.Lock()
	defer r.kvFile.lock.Unlock()

	file := r.kvFile.file
	if _, err := file.Seek(r.fileByteIndex, 0); err != nil {
		return nil, err
	}

	// read Key length
	var keyLength uint32
	if err := binary.Read(file, byteOrder, &keyLength); err != nil {
		// at end of file
		if err == io.EOF {
			return nil, nil
		}
		return nil, err
	}
	r.fileByteIndex += int64(binary.Size(keyLength))

	// read Key
	key := make([]byte, keyLength)
	if _, err := file.Read(key); err != nil {
		return nil, err
	}
	r.fileByteIndex += int64(keyLength)

	// read Value length
	var valueLength uint32
	if err := binary.Read(file, byteOrder, &valueLength); err != nil {
		return nil, err
	}
	r.fileByteIndex += int64(binary.Size(valueLength))

	// read Value
	value := make([]byte, valueLength)
	if _, err := file.Read(value); err != nil {
		return nil, err
	}
	r.fileByteIndex += int64(valueLength)

	return &KVPair{
		Key:   key,
		Value: value,
	}, nil
}
