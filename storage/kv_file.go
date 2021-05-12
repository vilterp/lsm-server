package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
)

var byteOrder binary.ByteOrder

func init() {
	byteOrder = binary.LittleEndian
}

// === file ===

type KVFile struct {
	lock sync.Mutex

	fileByteIndex int64
	file          *os.File
}

func NewKVFile(path string) (*KVFile, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	return &KVFile{
		fileByteIndex: 0,
		file:          file,
	}, nil
}

func (kvf *KVFile) AppendKVPair(key []byte, value []byte) (int64, error) {
	kvf.lock.Lock()
	defer kvf.lock.Unlock()

	beginningPos := kvf.fileByteIndex
	if err := kvf.appendLengthPrefixedBytes(key); err != nil {
		return 0, err
	}
	if err := kvf.appendLengthPrefixedBytes(value); err != nil {
		return 0, err
	}
	return beginningPos, nil
}

func (kvf *KVFile) appendLengthPrefixedBytes(bytes []byte) error {
	keyLength := uint32(len(bytes))
	if err := binary.Write(kvf.file, byteOrder, keyLength); err != nil {
		return err
	}
	kvf.fileByteIndex += int64(binary.Size(keyLength))
	if _, err := kvf.file.Write(bytes); err != nil {
		return err
	}
	kvf.fileByteIndex += int64(keyLength)
	return nil
}

func (kvf *KVFile) Truncate() error {
	kvf.lock.Lock()
	defer kvf.lock.Unlock()

	return kvf.file.Truncate(0)
}

// === reader ===

type KVFileReader struct {
	file          *os.File
	fileByteIndex int
}

type KVPair struct {
	Key          []byte
	Value        []byte
	OnDiskOffset int
}

func (kvf *KVFile) GetReader() (*KVFileReader, error) {
	file, err := os.Open(kvf.file.Name())
	if err != nil {
		return nil, err
	}

	return &KVFileReader{
		file:          file,
		fileByteIndex: 0,
	}, nil
}

func (r *KVFileReader) Seek(pos int64) error {
	r.fileByteIndex = int(pos)
	if _, err := r.file.Seek(pos, io.SeekStart); err != nil {
		return err
	}
	return nil
}

func (r *KVFileReader) readLengthPrefixedBytes() ([]byte, error) {
	// read length
	var length uint32
	if err := binary.Read(r.file, byteOrder, &length); err != nil {
		return nil, err
	}
	r.fileByteIndex += binary.Size(length)

	// read bytes
	bytes := make([]byte, length)
	if _, err := r.file.Read(bytes); err != nil {
		return nil, err
	}
	r.fileByteIndex += int(length)

	return bytes, nil
}

func (r *KVFileReader) Next() (*KVPair, error) {
	// TODO: may need to grab kvf's lock
	beginningPos := r.fileByteIndex

	key, err := r.readLengthPrefixedBytes()
	if err != nil {
		if err == io.EOF {
			return nil, nil
		}
		return nil, fmt.Errorf("error reading key: %v", err)
	}

	value, err := r.readLengthPrefixedBytes()
	if err != nil {
		return nil, fmt.Errorf("error reading value: %v", err)
	}

	return &KVPair{
		Key:          key,
		Value:        value,
		OnDiskOffset: beginningPos,
	}, nil
}
