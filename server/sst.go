package server

type SST struct {
	file   *KVFile
	reader *KVFileReader
	index  map[string]int64 // TODO: use bloom filter :P
}

func NewSST(file *KVFile) (*SST, error) {
	// TODO: read in index?
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

// TODO: write the index at the beginning so we don't have to read the whole file like this?
func (sst *SST) LoadIndex() error {
	if err := sst.reader.Seek(0); err != nil {
		return err
	}
	pos := int64(0)
	for {
		pair, size, err := sst.reader.Next()
		if err != nil {
			return err
		}
		if pair == nil {
			break
		}
		sst.index[string(pair.Key)] = pos
		pos += size
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
	kvPair, _, err := sst.reader.Next()
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
