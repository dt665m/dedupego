package dedupe

import (
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/dgraph-io/badger"
)

func TestDedupe(t *testing.T) {
	dd := NewDedupe("", 5*time.Second)
	key := make([]byte, 8)
	for i := 0; i < 10; i++ {
		binary.BigEndian.PutUint64(key, uint64(i))
		err := dd.TryAdd(key, key)
		if err != nil {
			panic(err)
		}
	}

	err := dd.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			v, err := item.Value()
			if err != nil {
				return err
			}
			fmt.Printf("key=%d, value=%d\n", binary.BigEndian.Uint64(k), binary.BigEndian.Uint64(v))
		}
		return nil
	})
	fmt.Println(err)

	time.Sleep(5 * time.Second)
	fmt.Println("waiting")

	dd.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			v, err := item.Value()
			if err != nil {
				return err
			}
			fmt.Printf("key=%d, value=%d\n", binary.BigEndian.Uint64(k), binary.BigEndian.Uint64(v))
		}
		return nil
	})
	fmt.Println(err)

	dd.GC()
	dd.db.Close()
}
