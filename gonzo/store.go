// This is just a prototype. Please do not use it.
package gonzo

import (
	"fmt"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/types"
	"gopkg.in/mgo.v2/bson"
)

func newStore(path string) (kv.Storage, error) {
	d := tikv.Driver{}
	return d.Open(path)
}

func (b *TikvBackend) loadSchema() error {
	return kv.RunInNewTxn(b.store, false, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		dbs, err := t.ListDatabases()
		if err != nil {
			return err
		}

		b.dbs = make(map[string]*TikvDB, len(dbs))
		for _, db := range dbs {
			newDB, err1 := b.newDB(db.Name.O)
			if err1 != nil {
				return err1
			}
			collections, err1 := t.ListTables(db.ID)
			if err1 != nil {
				return err1
			}
			newDB.collections = make(map[string]*TikvCollection, len(collections))
			for _, c := range collections {
				newDB.C(c.Name.O)
			}
		}
		return nil
	})
}

func (b *TikvBackend) newDB(name string) (*TikvDB, error) {
	db := NewTikvDB(b.store)
	dbInfo := &model.DBInfo{Name: model.CIStr{O: name}}
	err := kv.RunInNewTxn(db.store, false, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		id, err1 := t.GenGlobalID()
		if err1 != nil {
			return err1
		}

		db.id = id
		dbInfo.ID = id
		return t.CreateDatabase(dbInfo)
	})
	if err != nil {
		return nil, err
	}

	b.dbs[name] = db
	return db, nil
}

func (db *TikvDB) newC(name string) (*TikvCollection, error) {
	newC := &TikvCollection{store: db.store}
	c := &model.TableInfo{Name: model.CIStr{O: name}}
	err := kv.RunInNewTxn(db.store, false, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		id, err1 := t.GenGlobalID()
		if err1 != nil {
			return err1
		}

		newC.id = id
		c.ID = id
		return t.CreateTable(db.id, c)
	})
	if err != nil {
		return nil, err
	}

	db.collections[name] = newC
	newC.alloc = autoid.NewAllocator(db.store, db.id)
	newC.docPrefix = tablecodec.GenTableRecordPrefix(newC.id)
	return newC, nil
}

func (c *TikvCollection) insert(doc []byte) error {
	id, err := c.alloc.Alloc(c.id)
	if err != nil {
		return err
	}

	return kv.RunInNewTxn(c.store, false, func(txn kv.Transaction) error {
		key := tablecodec.EncodeRecordKey(c.docPrefix, id)
		value, err := tablecodec.EncodeRow([]types.Datum{types.NewDatum(doc)}, []int64{0})
		if err != nil {
			return err
		}
		return txn.Set(key, value)
	})
}

func convert(d types.Datum) (interface{}, bson.M, error) {
	if d.IsNull() {
		return "", nil, nil
	}
	doc := bson.M{}
	err := bson.Unmarshal(d.GetBytes(), &doc)
	if err != nil {
		return "", nil, err
	}

	id, ok := doc["_id"]
	if !ok {
		return "", nil, fmt.Errorf("data %v without id", doc)
	}
	return id, doc, nil
}

func (c *TikvCollection) patternMatch(pattern bson.M) ([]interface{}, error) {
	var docs []interface{}

	err := kv.RunInNewTxn(c.store, false, func(txn kv.Transaction) error {
		startKey := tablecodec.EncodeRecordKey(c.docPrefix, 0)
		it, err := txn.Seek(startKey)
		if err != nil {
			return err
		}
		defer it.Close()
		if !it.Valid() {
			return nil
		}

		valsMap := make(map[int64]*types.FieldType)
		valsMap[0] = types.NewFieldType(mysql.TypeString)
		for it.Valid() && it.Key().HasPrefix(c.docPrefix) {
			id, err := tablecodec.DecodeRowKey(it.Key())
			if err != nil {
				return err
			}
			vals, err := tablecodec.DecodeRow(it.Value(), valsMap)
			if err != nil {
				return err
			}

			_, doc, err := convert(vals[0])
			if err != nil {
				return err
			}
			if isPatternMatch(doc, pattern) {
				docs = append(docs, doc)
			}

			key := tablecodec.EncodeRecordKey(c.docPrefix, id)
			if err = kv.NextUntil(it, util.RowKeyPrefixFilter(key)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return docs, nil
}
