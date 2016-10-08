// This is just a prototype. Please do not use it.
package gonzo

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"strings"
	"sync"

	"github.com/ngaut/log"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"gopkg.in/mgo.v2/bson"
	"gopkg.in/tomb.v2"
)

const (
	ConnectionPrefix = "C"
	DocumentPrefix   = "D"
)

type TikvCollection struct {
	id        int64
	alloc     autoid.Allocator
	store     kv.Storage
	docPrefix kv.Key
}

type TikvDB struct {
	id          int64
	store       kv.Storage
	collections map[string]*TikvCollection // cache collections
	lastErr     interface{}

	mu sync.RWMutex
}

func NewTikvDB(s kv.Storage) *TikvDB {
	return &TikvDB{
		store:       s,
		collections: make(map[string]*TikvCollection),
	}
}

func (db *TikvDB) Empty() bool {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return len(db.collections) == 0
}

func (db *TikvDB) CNames() (result []string) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	for cname, _ := range db.collections {
		result = append(result, cname)
	}
	return result
}

func (db *TikvDB) C(name string) Collection {
	db.mu.Lock()
	defer db.mu.Unlock()
	result, ok := db.collections[name]
	if !ok {
		var err error
		result, err = db.newC(name)
		if err != nil {
			log.Errorf("get new collection %v err:%v", name, err)
			// TODO: handle the error
		}
	}
	return result
}

func (db *TikvDB) LastError() interface{} {
	db.mu.RLock()
	defer db.mu.RUnlock()
	log.Error("lastErr", db.lastErr)
	return db.lastErr
}

func (db *TikvDB) SetLastError(doc interface{}) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if doc == nil {
		doc = bson.D{}
	}
	db.lastErr = doc
}

// TODO: implement it.
func (c *TikvCollection) Id(id string) interface{} {
	return nil
}

// TODO: implement it.
func (c *TikvCollection) All() (result []interface{}) {
	return nil
}

func (c *TikvCollection) Match(pattern bson.M) []interface{} {
	if pattern == nil {
		return c.All()
	}

	docs, err := c.patternMatch(pattern)
	if err != nil {
		log.Errorf("match failed err:%v", err)
	}

	return docs
}

// TODO: implement it.
func (c *TikvCollection) Delete(pattern bson.M, limit int) int {
	return 0
}

func (c *TikvCollection) Insert(doc interface{}) error {
	mDoc, ok := doc.(bson.M)
	if !ok {
		return fmt.Errorf("cannot insert instance of this type: %v", doc)
	}
	bDoc, err := bson.Marshal(mDoc)
	if err != nil {
		return err
	}

	return c.insert(bDoc)
}

type TikvBackend struct {
	store kv.Storage
	dbs   map[string]*TikvDB
	t     *tomb.Tomb
}

func NewTikvBackend(t *tomb.Tomb, path string) (*TikvBackend, error) {
	s, err := newStore(fmt.Sprintf("tikv://%s", path))
	if err != nil {
		return nil, err
	}

	b := &TikvBackend{
		store: s,
		t:     t,
	}
	return b, b.loadSchema()
}

func (b *TikvBackend) DBNames() (result []string) {
	for dbname, _ := range b.dbs {
		result = append(result, dbname)
	}
	return result
}

func (b *TikvBackend) DB(name string) DB {
	result, ok := b.dbs[name]
	if !ok {
		var err error
		result, err = b.newDB(name)
		log.Errorf("get new database err:%v", err)
		// TODO: handle the error
	}
	return result
}

func (b *TikvBackend) HandleQuery(c net.Conn, query *OpQueryMsg) {
	log.Info("tikv backend handle query")
	if query.FullCollectionName == "admin.$cmd" {
		err := b.handleAdminCommand(c, query)
		if err != nil {
			log.Errorf("tikv backend handle query failed %v", err)
		}
		return
	}

	fields := strings.SplitN(query.FullCollectionName, ".", 2)
	if len(fields) < 2 {
		respError(c, query.RequestID, fmt.Errorf("malformed full collection name %q", query.FullCollectionName))
		return
	}
	dbname, cname := fields[0], fields[1]
	if strings.HasPrefix(cname, "system.") {
		b.handleSystemQuery(c, query, dbname, cname)
		return
	}
	db := b.DB(dbname)
	if cname == "$cmd" {
		err := b.handleDBCommand(c, db, query)
		if err != nil {
			log.Errorf("tikv backend handle query failed %v", err)
		}
		return
	}
	coll := db.C(cname)

	var results []interface{}
	if match, ok := query.Get("$query"); ok {
		matchM, err := asBsonM(match)
		if err != nil {
			respError(c, query.RequestID, err)
		}
		results = append(results, coll.Match(matchM)...)
	} else if len(query.Doc) == 0 {
		results = append(results, coll.All()...)
	} else {
		results = append(results, coll.Match(query.Doc.Map())...)
	}
	respDoc(c, query.RequestID, results...)
}

// TODO: implement it.
func (b *TikvBackend) HandleUpdate(c net.Conn, update *OpUpdateMsg) {
	if strings.HasPrefix(update.FullCollectionName, "admin.") {
		respError(c, update.RequestID, fmt.Errorf("update not supported on admin.*"))
		return
	}

	fields := strings.SplitN(update.FullCollectionName, ".", 2)
	if len(fields) < 2 {
		respError(c, update.RequestID, fmt.Errorf("malformed full collection name %q", update.FullCollectionName))
		return
	}
	dbname, cname := fields[0], fields[1]
	if strings.HasPrefix(cname, "system.") {
		respError(c, update.RequestID, fmt.Errorf("update not supported on %q", update.FullCollectionName))
		return
	}
	db := b.DB(dbname)
	coll := db.C(cname)
	matched := coll.Match(update.Selector)

	if update.Flags&UpdateFlagMultiUpdate == 0 && len(matched) > 1 {
		matched = matched[:1]
	}

	result := &WriteResult{
		N: len(matched),
	}

	for _, match := range matched {
		err := applyUpdate(update.Update, match.(bson.M))
		if err != nil {
			respError(c, update.RequestID, err)
			return
		}
		result.UpdatedExisting = true
	}

	if update.Flags&UpdateFlagUpsert != 0 && result.N == 0 {
		id, ok := update.Update["_id"]
		if !ok {
			id = bson.NewObjectId()
			update.Update["_id"] = id
		}
		// TODO: Save to TiKV
		err := coll.Insert(update.Update)
		if err != nil {
			db.SetLastError(errReply(err))
			return
		}
		result.Upserted = id
	}
	db.SetLastError(errReply(nil))
}

func (b *TikvBackend) HandleDelete(c net.Conn, deleteMsg *OpDeleteMsg) {
	if strings.HasPrefix(deleteMsg.FullCollectionName, "admin.") {
		respError(c, deleteMsg.RequestID, fmt.Errorf("delete not supported on admin.*"))
		return
	}

	fields := strings.SplitN(deleteMsg.FullCollectionName, ".", 2)
	if len(fields) < 2 {
		respError(c, deleteMsg.RequestID, fmt.Errorf("malformed full collection name %q", deleteMsg.FullCollectionName))
		return
	}
	dbname, cname := fields[0], fields[1]
	if strings.HasPrefix(cname, "system.") {
		respError(c, deleteMsg.RequestID, fmt.Errorf("delete %q not supported on %q", deleteMsg.Selector, deleteMsg.FullCollectionName))
		return
	}
	db := b.DB(dbname)
	coll := db.C(cname)
	limit := 0
	if deleteMsg.Flags&DeleteFlagSingleRemove != 0 {
		limit = 1
	}
	n := coll.Delete(deleteMsg.Selector, limit)
	// TODO: enforce safe mode with int result of the above
	db.SetLastError(&WriteResult{
		N: n,
	})
}

func (b *TikvBackend) HandleInsert(c net.Conn, insert *OpInsertMsg) {
	log.Info("tikv backend handle insert")
	if strings.HasPrefix(insert.FullCollectionName, "admin.") {
		respError(c, insert.RequestID, fmt.Errorf("insert not supported on admin.*"))
		return
	}

	fields := strings.SplitN(insert.FullCollectionName, ".", 2)
	if len(fields) < 2 {
		respError(c, insert.RequestID, fmt.Errorf("malformed full collection name %q", insert.FullCollectionName))
		return
	}
	dbname, cname := fields[0], fields[1]
	if strings.HasPrefix(cname, "system.") {
		respError(c, insert.RequestID, fmt.Errorf("insert %q not supported on %q", insert.Docs, insert.FullCollectionName))
		return
	}
	db := b.DB(dbname)
	coll := db.C(cname)
	for _, doc := range insert.Docs {
		err := coll.Insert(doc)
		db.SetLastError(errReply(err))
		if err != nil {
			log.Errorf("tikv backend handle insert err:%v", err)
			return
		}
	}
}

func (b *TikvBackend) handleSystemQuery(c net.Conn, query *OpQueryMsg, dbname, cname string) {
	switch cname {
	case "system.namespaces":
		var result []interface{}
		for _, name := range b.DB(dbname).CNames() {
			result = append(result, bson.D{{"name", name}})
		}
		respDoc(c, query.RequestID, result...)
		return
	}
	respError(c, query.RequestID, fmt.Errorf(
		"unsupported system query on %s: %v", query.FullCollectionName, query.Doc))
}

func (b *TikvBackend) handleDBCommand(c net.Conn, db DB, query *OpQueryMsg) error {
	var err error
	switch cmd, arg := query.Command(); cmd {
	case "getLastError", "getlasterror":
		return respDoc(c, query.RequestID, db.LastError())
	case "getnonce":
		nonce := make([]byte, 32)
		_, err := rand.Reader.Read(nonce[:])
		if err != nil {
			return err
		}
		return respDoc(c, query.RequestID, markOk(bson.D{
			{"nonce", hex.EncodeToString(nonce)},
		}))
	case "authenticate":
		// It's a test database, let everyone in.
		return respDoc(c, query.RequestID, markOk(nil))
	case "count":
		cname, ok := arg.(string)
		if !ok {
			return respError(c, query.RequestID, fmt.Errorf("malformed count command: %q", query.Doc))
		}
		coll := db.C(cname)
		var matchM bson.M
		if q, ok := query.Get("query"); ok {
			matchM, err = asBsonM(q)
			if err != nil {
				return respError(c, query.RequestID, err)
			}
		}
		return respDoc(c, query.RequestID, markOk(bson.D{{"n", len(coll.Match(matchM))}}))
	}
	return respError(c, query.RequestID, fmt.Errorf("unsupported db command: %v", query))
}

func (b *TikvBackend) handleAdminCommand(c net.Conn, query *OpQueryMsg) error {
	switch cmd, arg := query.Command(); cmd {
	case "getLog":
		var msg bson.D
		switch logName := arg.(string); logName {
		case "*":
			msg = markOk(bson.D{{"names", []string{"startupWarnings"}}})
		case "startupWarnings":
			msg = markOk(bson.D{
				{"totalLinesWritten", 0},
				{"log", []string{}},
			})
		default:
			msg = errReply(fmt.Errorf("log not found: %q", logName))
		}
		return respDoc(c, query.RequestID, msg)
	case "listDatabases":
		var dbinfos []bson.D
		for _, dbname := range b.DBNames() {
			dbinfos = append(dbinfos, bson.D{
				{"name", dbname},
				{"empty", b.DB(dbname).Empty()},
			})
		}
		return respDoc(c, query.RequestID, markOk(bson.D{
			{"databases", dbinfos},
		}))
	case "replSetGetStatus":
		members := make([]bson.M, 0)

		member := bson.M{}
		member["_id"] = 0
		member["name"] = "localhost:47017"
		member["health"] = 1
		member["state"] = 1
		member["stateStr"] = "PRIMARY"
		member["self"] = true
		members = append(members, member)

		r := bson.D{
			{"set", "repl"},
			{"date", bson.Now()},
			{"myState", 1},
			{"members", members},
		}
		return respDoc(c, query.RequestID, r)
	case "shutdown":
		log.Info("shutdown requested")
		b.t.Kill(nil)
		return c.Close()
	case "whatsmyuri":
		return respDoc(c, query.RequestID, bson.D{{"you", c.RemoteAddr().String()}})
	case "ismaster":
		return respDoc(c, query.RequestID, markOk(bson.D{{"ismaster", true}}))
	case "getnonce":
		nonce := make([]byte, 32)
		_, err := rand.Reader.Read(nonce[:])
		if err != nil {
			return err
		}
		return respDoc(c, query.RequestID, markOk(bson.D{
			{"nonce", hex.EncodeToString(nonce)},
		}))
	case "ping":
		return respDoc(c, query.RequestID, markOk(nil))
	}
	return respError(c, query.RequestID, fmt.Errorf("unsupported admin command: %v", query))
}
