package storage

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/emirpasic/gods/maps/treemap"
)

type LSM struct {
	lock sync.Mutex

	dataDir   string
	nextSSTID int

	wal      *KVFile
	memtable *treemap.Map
	ssts     []*SST // newest last
}

func NewLSM(dataDir string) (*LSM, error) {
	// create data dir if it doesn't exist
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		if err := os.Mkdir(dataDir, 0700); err != nil {
			return nil, err
		}
	}

	wal, err := NewKVFile(path.Join(dataDir, "wal.kv"))
	if err != nil {
		return nil, err
	}
	lsm := &LSM{
		wal:      wal,
		dataDir:  dataDir,
		memtable: treemap.NewWithStringComparator(),
	}
	if err := lsm.loadWALIntoMemtable(); err != nil {
		return nil, fmt.Errorf("loading WAL: %v", err)
	}
	if err := lsm.loadSSTs(); err != nil {
		return nil, fmt.Errorf("loading SSTs: %v", err)
	}
	return lsm, nil
}

type EntriesStats struct {
	MemtableEntries int
	SSTEntries      int
}

func (lsm *LSM) EntriesStats() EntriesStats {
	lsm.lock.Lock()
	defer lsm.lock.Unlock()

	sstEntries := 0
	for _, sst := range lsm.ssts {
		sstEntries += sst.NumEntries()
	}
	return EntriesStats{
		SSTEntries:      sstEntries,
		MemtableEntries: lsm.memtable.Size(),
	}
}

func (lsm *LSM) loadSSTs() error {
	// TODO: get these in the right order (i.e. sort by filename)
	files, err := ioutil.ReadDir(lsm.dataDir)
	if err != nil {
		return err
	}
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), "sst.kv") {
			continue
		}
		if err := lsm.loadSST(file.Name()); err != nil {
			return fmt.Errorf("loading %s: %v", file.Name(), err)
		}
	}
	lsm.nextSSTID = len(lsm.ssts) // TODO: not valid once we do compaction
	return nil
}

func (lsm *LSM) loadSST(file string) error {
	kvFile, err := NewKVFile(path.Join(lsm.dataDir, file))
	if err != nil {
		return err
	}
	sst, err := NewSST(kvFile)
	if err != nil {
		return err
	}
	if err := sst.LoadIndex(); err != nil {
		return fmt.Errorf("loading index: %v", err)
	}
	lsm.ssts = append(lsm.ssts, sst)
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
		lsm.memtable.Put(string(pair.Key), pair.Value)
	}
	return nil
}

func (lsm *LSM) Get(key []byte) ([]byte, bool, error) {
	lsm.lock.Lock()
	defer lsm.lock.Unlock()

	memValue, ok := lsm.memtable.Get(string(key))
	if ok {
		return memValue.([]byte), true, nil
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
	lsm.memtable.Put(string(key), value)
	if lsm.memtable.Size() > MemtableSizeThreshold {
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
	newSST, err := WriteSST(path.Join(lsm.dataDir, name), lsm.memtable)
	if err != nil {
		return err
	}
	lsm.ssts = append(lsm.ssts, newSST)

	// clear the memtable
	lsm.memtable.Clear()
	if err := lsm.wal.Truncate(); err != nil {
		return err
	}
	return nil
}

// TODO: compaction
