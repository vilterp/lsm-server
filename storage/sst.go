package storage

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
)

type SST struct {
	file      *KVFile
	reader    *KVFileReader
	index     map[string]int64
	dataStart int64
}

func NewSST(file *KVFile) (*SST, error) {
	reader, err := file.GetReader()
	if err != nil {
		return nil, err
	}
	return &SST{
		file:   file,
		reader: reader,
		index:  map[string]int64{},
	}, nil
}

func (sst *SST) LoadIndex() error {
	indexKVPair, err := sst.reader.Next()
	if err != nil {
		if err == io.EOF {
			// this is fine, just means it's a new, empty SST
			return nil
		} else {
			return err
		}
	}
	if err := json.Unmarshal(indexKVPair.Value, &sst.index); err != nil {
		return fmt.Errorf("unmarshalling JSON: %v", err)
	}
	sst.dataStart = int64(sst.reader.fileByteIndex)
	return nil
}

func (sst *SST) NumEntries() int {
	return len(sst.index)
}

func (sst *SST) ReadKey(key []byte) ([]byte, bool, error) {
	rawIndex, ok := sst.index[string(key)]
	if !ok {
		// not found
		return nil, false, nil
	}
	index := rawIndex + sst.dataStart
	if err := sst.reader.Seek(index); err != nil {
		return nil, false, err
	}
	kvPair, err := sst.reader.Next()
	if err != nil {
		return nil, false, err
	}
	return kvPair.Value, true, nil
}

func WriteSST(name string, memtable map[string][]byte) (*SST, error) {
	kvFile, err := NewKVFile(name)
	if err != nil {
		return nil, err
	}
	sst, err := NewSST(kvFile)
	if err != nil {
		return nil, err
	}

	// get stable order for keys
	var keys []string
	for key, _ := range memtable {
		keys = append(keys, key)
	}

	// get offsets
	offsets := map[string]int{}
	pos := 0
	for _, key := range keys {
		offsets[key] = pos
		// TODO: DRY up
		pos += binary.Size(uint32(len(key)))
		pos += len(key)
		value := memtable[key]
		pos += binary.Size(uint32(len(value)))
		pos += len(value)
	}

	encodedIndex, err := json.Marshal(offsets)
	if err != nil {
		return nil, err
	}
	if _, err := kvFile.AppendKVPair([]byte("index"), encodedIndex); err != nil {
		return nil, err
	}

	// write keys
	// TODO: sort? lol
	for key, value := range memtable {
		startPos, err := kvFile.AppendKVPair([]byte(key), value)
		if err != nil {
			return nil, err
		}
		sst.index[key] = startPos
	}
	return sst, nil
}
