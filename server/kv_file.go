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
	pos     int64
}

func NewKVFile(path string) (*KVFile, error) {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	kvf := &KVFile{
		file:    file,
		encoder: gob.NewEncoder(file),
	}
	if err := kvf.updatePos(); err != nil {
		return nil, err
	}
	return kvf, nil
}

func (kvf *KVFile) updatePos() error {
	pos, err := kvf.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	kvf.pos = pos
	return nil
}

// AppendKVPair returns the offset in the file it starts at
func (kvf *KVFile) AppendKVPair(key []byte, value []byte) (int64, error) {
	startPos := kvf.pos
	if err := kvf.encoder.Encode(KVPair{
		Key:   key,
		Value: value,
	}); err != nil {
		return 0, err
	}
	if err := kvf.updatePos(); err != nil {
		return 0, err
	}
	return startPos, nil
}

func (kvf *KVFile) Truncate() error {
	return os.Truncate(kvf.file.Name(), 0)
}

// === reader ===

type KVFileReader struct {
	file    *os.File
	pos     int64
	decoder *gob.Decoder
}

func (kvf *KVFile) GetReader() (*KVFileReader, error) {
	file, err := os.Open(kvf.file.Name())
	if err != nil {
		return nil, err
	}
	return &KVFileReader{
		file:    file,
		pos:     0,
		decoder: gob.NewDecoder(file),
	}, nil
}

// Next returns the item (or nil if we're at the end), the size in
// bytes of the item on disk, or an error if there was one
func (r *KVFileReader) Next() (*KVPair, int64, error) {
	startingPos := r.pos

	var out KVPair
	if err := r.decoder.Decode(&out); err != nil {
		if err == io.EOF {
			return nil, 0, nil
		}
		return nil, 0, err
	}

	newPos, err := r.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, 0, err
	}
	r.pos = newPos

	size := newPos - startingPos
	return &out, size, nil
}

func (r *KVFileReader) Seek(pos int64) error {
	if _, err := r.file.Seek(pos, io.SeekStart); err != nil {
		return err
	}
	r.pos = pos
	return nil
}
