package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"os"
	"path/filepath"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	kv            *badger.DB
	config        *config.Config
	storageReader *StandAloneStorageReader
}

type StandAloneStorageReader struct {
	txn      *badger.Txn
	iterator *engine_util.BadgerIterator
}

func NewStandAloneStorageReader(kv *badger.DB) *StandAloneStorageReader {
	txn := kv.NewTransaction(false)
	return &StandAloneStorageReader{txn: txn}
}

func (sr *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, _ := engine_util.GetCFFromTxn(sr.txn, cf, key)
	return val, nil
}

func (sr *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	sr.iterator = engine_util.NewCFIterator(cf, sr.txn)
	return sr.iterator
}

func (sr *StandAloneStorageReader) Close() {
	sr.txn.Discard()
	if sr.iterator != nil {
		sr.iterator.Close()
	}
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	kvPath := filepath.Join(conf.DBPath, "kv")
	os.MkdirAll(kvPath, os.ModePerm)

	kvDB := engine_util.CreateDB(kvPath, false)
	return &StandAloneStorage{kv: kvDB, config: conf, storageReader: nil}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	if s.storageReader != nil {
		s.storageReader.Close()
	}
	if err := s.kv.Close(); err != nil {
		return err
	}
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	s.storageReader = NewStandAloneStorageReader(s.kv)
	return s.storageReader, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	for _, m := range batch {
		switch m.Data.(type) {
		case storage.Put:
			err := engine_util.PutCF(s.kv, m.Cf(), m.Key(), m.Value())
			if err != nil {
				return err
			}
		case storage.Delete:
			err := engine_util.DeleteCF(s.kv, m.Cf(), m.Key())
			if err != nil {
				return err
			}
		}
	}
	return nil
}
