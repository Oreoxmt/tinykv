package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"path/filepath"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	engine *engine_util.Engines
}

type StandAloneStorageReader struct {
	txn *badger.Txn
}

type StandAloneStorageDBIterator struct {
	iterator *engine_util.BadgerIterator
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	dbPath := conf.DBPath
	kvPath := filepath.Join(dbPath, "kv")
	kvDB := engine_util.CreateDB(kvPath, false)
	raftPath := filepath.Join(dbPath, "raft")
	raftDB := engine_util.CreateDB(raftPath, true)
	engine := engine_util.NewEngines(kvDB, raftDB, kvPath, raftPath)
	newStorage := StandAloneStorage{engine}
	return &newStorage
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	destroyErr := s.engine.Destroy()
	if destroyErr != nil {
		return destroyErr
	}
	s.engine = nil
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	txn := s.engine.Kv.NewTransaction(false)
	reader := StandAloneStorageReader{txn}
	return &reader, nil
}

func (r *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	if val == nil {
		return val, nil
	}
	return val, err
}

func (r *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return &StandAloneStorageDBIterator{engine_util.NewCFIterator(cf, r.txn)}
}

func (i *StandAloneStorageDBIterator) Item() engine_util.DBItem {
	return i.iterator.Item()
}

func (i *StandAloneStorageDBIterator) Valid() bool {
	return i.iterator.Valid()
}

func (i *StandAloneStorageDBIterator) Next() {
	i.iterator.Next()
}

func (i *StandAloneStorageDBIterator) Seek(key []byte) {
	i.iterator.Seek(key)
}

func (i *StandAloneStorageDBIterator) Close() {
	i.iterator.Close()
}

func (r *StandAloneStorageReader) Close() {
	r.txn.Discard()
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	writeBatch := engine_util.WriteBatch{}
	for _, item := range batch {
		key := item.Key()
		value := item.Value()
		cf := item.Cf()
		if value == nil {
			writeBatch.DeleteCF(cf, key)
		} else {
			writeBatch.SetCF(cf, key, value)
		}
	}
	err := s.engine.WriteKV(&writeBatch)
	if err != nil {
		return err
	}
	return nil
}
