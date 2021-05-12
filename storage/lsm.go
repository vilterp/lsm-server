package storage

import (
	"fmt"
	"io/ioutil"
	"log"
	"path"
	"strings"
	"sync"
)

type LSM struct {
	lock sync.Mutex

	sstDir    string
	nextSSTID int

	wal      *KVFile
	memtable map[string][]byte
	ssts     []*SST // newest last
}

func NewLSM(wal *KVFile, sstDir string) (*LSM, error) {
	lsm := &LSM{
		wal:      wal,
		sstDir:   sstDir,
		memtable: map[string][]byte{},
	}
	if err := lsm.loadWALIntoMemtable(); err != nil {
		return nil, err
	}
	if err := lsm.loadSSTs(); err != nil {
		return nil, err
	}
	return lsm, nil
}

func (lsm *LSM) loadSSTs() error {
	// TODO: get these in the right order (i.e. sort by filename)
	files, err := ioutil.ReadDir(lsm.sstDir)
	if err != nil {
		return err
	}
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), "sst.kv") {
			continue
		}
		kvFile, err := NewKVFile(path.Join(lsm.sstDir, file.Name()))
		if err != nil {
			return err
		}
		sst, err := NewSST(kvFile)
		if err != nil {
			return err
		}
		if err := sst.LoadIndex(); err != nil {
			return err
		}
		lsm.ssts = append(lsm.ssts, sst)
	}
	lsm.nextSSTID = len(lsm.ssts) // TODO: not valid once we do compaction
	return nil
}

func (lsm *LSM) loadWALIntoMemtable() error {
	lsm.lock.Lock()
	defer lsm.lock.Unlock()

	reader, err := lsm.wal.GetReader()
	if err != nil {
		return err
	}
	for {
		pair, err := reader.Next()
		if err != nil {
			return err
		}
		if pair == nil {
			break
		}
		lsm.memtable[string(pair.Key)] = pair.Value
	}
	return nil
}

func (lsm *LSM) Get(key []byte) ([]byte, bool, error) {
	lsm.lock.Lock()
	defer lsm.lock.Unlock()

	memValue, ok := lsm.memtable[string(key)]
	if ok {
		return memValue, true, nil
	}
	// iterate from newest to oldest
	for i := len(lsm.ssts) - 1; i >= 0; i-- {
		sst := lsm.ssts[i]
		value, found, err := sst.ReadKey(key)
		if err != nil {
			return nil, false, err
		}
		if found {
			return value, true, nil
		}
	}
	return nil, false, nil
}

const MemtableSizeThreshold = 10 // very low for testing

func (lsm *LSM) Put(key []byte, value []byte) error {
	lsm.lock.Lock()
	defer lsm.lock.Unlock()

	if _, err := lsm.wal.AppendKVPair(key, value); err != nil {
		return err
	}
	lsm.memtable[string(key)] = value
	if len(lsm.memtable) > MemtableSizeThreshold {
		// TODO: maybe this should be done async, and log errors?
		if err := lsm.flushMemtable(); err != nil {
			return err
		}
	}
	return nil
}

func (lsm *LSM) doFlushMemtable() {
	if err := lsm.flushMemtable(); err != nil {
		log.Println("error flushing memtable: ", err)
	}
}

func (lsm *LSM) flushMemtable() error {
	// write new sst
	name := fmt.Sprintf("%d.sst.kv", lsm.nextSSTID)
	lsm.nextSSTID++
	newSST, err := WriteSST(path.Join(lsm.sstDir, name), lsm.memtable)
	if err != nil {
		return err
	}
	lsm.ssts = append(lsm.ssts, newSST)

	// clear the memtable
	lsm.memtable = map[string][]byte{}
	if err := lsm.wal.Truncate(); err != nil {
		return err
	}
	return nil
}

// TODO: compaction
