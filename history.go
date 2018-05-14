package qhistory

import (
	"github.com/dgraph-io/badger"
	"time"
)

type History interface {
	Add(key []byte, record *Record, ttl time.Duration) error
	Scan(key []byte, timestamp time.Time, limit int) (records []*Record, err error)
	Close() error
}

type history struct {
	db *badger.DB
}

func NewHistory(dir string) (History, error) {
	opts := badger.DefaultOptions
	opts.Dir = dir
	opts.ValueDir = dir

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return &history{
		db: db,
	}, nil
}

func (h *history) Add(key []byte, record *Record, ttl time.Duration) error {
	for {
		txn := h.db.NewTransaction(true)

		txn.SetWithTTL(makeKey([]byte("r"), key, record.Time), record.Data, ttl)

		if err := txn.Commit(nil); err != badger.ErrConflict {
			return err
		}

		time.Sleep(50 * time.Millisecond)
	}
}

func (h *history) Scan(key []byte, timestamp time.Time, limit int) (records []*Record, err error) {
	txn := h.db.NewTransaction(false)

	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()

	prefix := append([]byte("r"), key...)

	var data []byte
	for it.Seek(makeKey([]byte("r"), key, timestamp)); it.ValidForPrefix(prefix); it.Next() {
		data, err = it.Item().Value()
		if err != nil {
			return
		}

		key := it.Item().Key()

		records = append(records, &Record{
			Data: data,
			Time: extractTime(key),
		})
	}

	return
}

func (h *history) Close() error {
	return h.db.Close()
}
