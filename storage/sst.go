package storage

type SST struct {
	file   *KVFile
	reader *KVFileReader
	index  map[string]int64 // TODO: use bloom filter :P
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

func (sst *SST) NumEntries() int {
	return len(sst.index)
}

// LoadIndex populates the index.
// TODO: write the index at the beginning so we don't have to read the whole file like this?
func (sst *SST) LoadIndex() error {
	if err := sst.reader.Seek(0); err != nil {
		return err
	}
	for {
		pair, err := sst.reader.Next()
		if err != nil {
			return err
		}
		if pair == nil {
			break
		}
		sst.index[string(pair.Key)] = int64(pair.OnDiskOffset)
	}
	return nil
}

func (sst *SST) ReadKey(key []byte) ([]byte, bool, error) {
	index, ok := sst.index[string(key)]
	if !ok {
		// not found
		return nil, false, nil
	}
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

	// write num keys
	// write index

	// write keys
	// TODO: uhh... sort?
	for key, value := range memtable {
		startPos, err := kvFile.AppendKVPair([]byte(key), value)
		if err != nil {
			return nil, err
		}
		sst.index[key] = startPos
	}
	return sst, nil
}
