package dedupe

import (
	"fmt"
	"log"
	"time"

	"github.com/dgraph-io/badger"
)

type Dedupe struct {
	db  *badger.DB
	ttl time.Duration
}

func NewDedupe(dir string, ttl time.Duration) *Dedupe {
	// Open the Badger database located in the /tmp/badger directory.
	// It will be created if it doesn't exist.
	opts := badger.DefaultOptions
	if dir == "" {
		log.Println("using default dir /badger")
		opts.Dir = "./badger"
		opts.ValueDir = "./badger"
	} else {
		opts.Dir = dir
		opts.ValueDir = dir
	}

	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	if err != nil {
		panic(err)
	}

	return &Dedupe{db, ttl}
}

func (d *Dedupe) TryAdd(key []byte, val []byte) error {
	return d.db.Update(func(txn *badger.Txn) error {
		_, err := txn.Get(key)
		if err != badger.ErrKeyNotFound {
			return fmt.Errorf("duplicate key: %s", string(key))
		}

		return txn.SetWithTTL(key, val, d.ttl)
	})
}

func (d *Dedupe) GC() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		err := d.db.RunValueLogGC(0.7)
		fmt.Println("db: ", err)
		if err != nil {
			return
		}
	}
	fmt.Println("finished gc")
}

func (d *Dedupe) Close() error {
	return d.db.Close()
}
