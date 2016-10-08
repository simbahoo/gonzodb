package gonzo

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"strings"
	"sync"

	"github.com/ngaut/log"
	"gopkg.in/mgo.v2/bson"
	"gopkg.in/tomb.v2"
)

type Backend interface {
	HandleQuery(c net.Conn, query *OpQueryMsg)
	HandleInsert(c net.Conn, insert *OpInsertMsg)
	HandleUpdate(c net.Conn, update *OpUpdateMsg)
	HandleDelete(c net.Conn, deleteMsg *OpDeleteMsg)

	DBNames() []string
	DB(name string) DB
}

type DB interface {
	Empty() bool
	CNames() []string
	C(name string) Collection

	LastError() interface{}
	SetLastError(doc interface{})
}

type Collection interface {
	Id(id string) interface{}
	All() []interface{}
	Match(pattern bson.M) []interface{}
	Insert(item interface{}) error
	Delete(pattern bson.M, limit int) int
}

type MemoryCollection struct {
	docs map[string]bson.M

	mu sync.RWMutex
}

type MemoryDB struct {
	collections map[string]*MemoryCollection
	lastErr     interface{}

	mu sync.RWMutex
}

func NewMemoryDB() *MemoryDB {
	return &MemoryDB{collections: make(map[string]*MemoryCollection)}
}

func (db *MemoryDB) Empty() bool {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return len(db.collections) == 0
}

func (db *MemoryDB) CNames() (result []string) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	for cname, _ := range db.collections {
		result = append(result, cname)
	}
	return result
}

func (db *MemoryDB) C(name string) Collection {
	db.mu.Lock()
	defer db.mu.Unlock()
	result, ok := db.collections[name]
	if !ok {
		result = &MemoryCollection{docs: make(map[string]bson.M)}
		db.collections[name] = result
	}
	return result
}

func (db *MemoryDB) LastError() interface{} {
	db.mu.RLock()
	defer db.mu.RUnlock()
	log.Info("lastErr", db.lastErr)
	return db.lastErr
}

func (db *MemoryDB) SetLastError(doc interface{}) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if doc == nil {
		doc = bson.D{}
	}
	db.lastErr = doc
}

func (c *MemoryCollection) Id(id string) interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, item := range c.docs {
		mitem := item
		if match, ok := mitem["_id"]; ok && match == id {
			return item
		}
	}
	return nil
}

func (c *MemoryCollection) All() (result []interface{}) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, doc := range c.docs {
		result = append(result, doc)
	}
	return result
}

func (c *MemoryCollection) Match(pattern bson.M) (result []interface{}) {
	if pattern == nil {
		return c.All()
	}

	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, doc := range c.docs {
		if isPatternMatch(doc, pattern) {
			result = append(result, doc)
		}
	}

	return result
}

func (c *MemoryCollection) Delete(pattern bson.M, limit int) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	var removeIds []string
	for id, doc := range c.docs {
		if isPatternMatch(doc, pattern) {
			removeIds = append(removeIds, id)
			if limit > 0 && limit >= len(removeIds) {
				break
			}
		}
	}
	for _, id := range removeIds {
		delete(c.docs, id)
	}
	return len(removeIds)
}

func isPatternMatch(doc, pattern bson.M) bool {
	for matchKey, matchValue := range pattern {
		value, ok := doc[matchKey]
		if !ok || matchValue != value {
			return false
		}
	}
	return true
}

func (c *MemoryCollection) Insert(doc interface{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	mdoc, ok := doc.(bson.M)
	if !ok {
		return fmt.Errorf("cannot insert instance of this type: %v", doc)
	}

	var idStr string
	if id, ok := mdoc["_id"]; ok {
		idStr = fmt.Sprintf("%s", id)
	} else {
		oid := bson.NewObjectId()
		mdoc["_id"] = oid
		idStr = oid.String()
	}
	c.docs[idStr] = mdoc

	return nil
}

type MemoryBackend struct {
	dbs map[string]*MemoryDB
	t   *tomb.Tomb
}

func NewMemoryBackend(t *tomb.Tomb) *MemoryBackend {
	return &MemoryBackend{
		dbs: make(map[string]*MemoryDB),
		t:   t,
	}
}

func (b *MemoryBackend) DBNames() (result []string) {
	for dbname, _ := range b.dbs {
		result = append(result, dbname)
	}
	return result
}

func (b *MemoryBackend) DB(name string) DB {
	result, ok := b.dbs[name]
	if !ok {
		result = NewMemoryDB()
		b.dbs[name] = result
	}
	return result
}

func asBsonM(v interface{}) (bson.M, error) {
	if v == nil {
		return nil, nil
	}
	switch b := v.(type) {
	case bson.D:
		return b.Map(), nil
	case bson.M:
		return b, nil
	}
	return nil, fmt.Errorf("cannot resolve %q to bson.M", v)
}

func (b *MemoryBackend) HandleQuery(c net.Conn, query *OpQueryMsg) {
	log.Info("memory backend handle query")
	if query.FullCollectionName == "admin.$cmd" {
		err := b.handleAdminCommand(c, query)
		if err != nil {
			log.Errorf("memory backend handle query failed %v", err)
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
			log.Errorf("memory backend handle query failed %v", err)
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

func (b *MemoryBackend) HandleUpdate(c net.Conn, update *OpUpdateMsg) {
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
		err := coll.Insert(update.Update)
		if err != nil {
			db.SetLastError(errReply(err))
			return
		}
		result.Upserted = id
	}
	db.SetLastError(errReply(nil))
}

func applyUpdate(spec, target bson.M) error {
	replace := false
	for k, v := range spec {
		unsuppErr := fmt.Errorf("unsupported update operator: %q", k)
		switch k {
		case "$currentDate":
			return unsuppErr
		case "$inc":
			return unsuppErr
		case "$max":
			return unsuppErr
		case "$min":
			return unsuppErr
		case "$mul":
			return unsuppErr
		case "$rename":
			return unsuppErr
		case "$setOnInsert":
			return unsuppErr
		case "$set":
			set, err := asBsonM(v)
			if err != nil {
				return err
			}
			for setK, setV := range set {
				target[setK] = setV
			}
		case "$unset":
			return unsuppErr
		default:
			if !replace {
				replace = true
				for tk, _ := range target {
					if tk != "_id" {
						delete(target, tk)
					}
				}
			}
			target[k] = v
		}
	}
	return nil
}

func (b *MemoryBackend) HandleDelete(c net.Conn, deleteMsg *OpDeleteMsg) {
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

func (b *MemoryBackend) HandleInsert(c net.Conn, insert *OpInsertMsg) {
	log.Info("memory backend handle insert")
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
			log.Errorf("memory backend handle insert failed %v", err)
			return
		}
	}
}

// TODO: implement Collection interface instead.
func (b *MemoryBackend) handleSystemQuery(c net.Conn, query *OpQueryMsg, dbname, cname string) {
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

func (b *MemoryBackend) handleDBCommand(c net.Conn, db DB, query *OpQueryMsg) error {
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

func (b *MemoryBackend) handleAdminCommand(c net.Conn, query *OpQueryMsg) error {
	log.Infof("memory admin db cmd")
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
