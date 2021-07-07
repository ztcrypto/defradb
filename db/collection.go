// Copyright 2020 Source Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
package db

import (
	"encoding/json"
	"fmt"

	"github.com/fxamacker/cbor/v2"
	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/core"
	"github.com/sourcenetwork/defradb/db/base"
	"github.com/sourcenetwork/defradb/document"
	"github.com/sourcenetwork/defradb/document/key"
	"github.com/sourcenetwork/defradb/merkle/crdt"

	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	mh "github.com/multiformats/go-multihash"
	"github.com/pkg/errors"
)

var (
	collectionSeqKey = "collection"
	collectionNs     = ds.NewKey("/collection")
)

var (
	ErrDocumentAlreadyExists = errors.New("A document with the given key already exists")
	ErrUnknownCRDTArgument   = errors.New("Invalid CRDT arguments")
	ErrUnknownCRDT           = errors.New("")
)

var _ client.Collection = (*Collection)(nil)

// Collection stores data records at Documents, which are gathered
// together under a collection name. This is analgeous to SQL Tables.
type Collection struct {
	db  *DB
	txn *Txn

	colID    uint32
	colIDKey core.Key

	desc      base.CollectionDescription
	hasSchema bool
}

// @todo: Move the base Descriptions to an internal API within the db/ package.
// @body: Currently, the New/Create Collection APIs accept CollectionDescriptions
// as params. We want these Descriptions objects to be low level descriptions, and
// to be auto generated based on a more controllable and user friendly
// CollectionOptions object.

// NewCollection returns a pointer to a newly instanciated DB Collection
func (db *DB) newCollection(desc base.CollectionDescription) (*Collection, error) {
	if desc.Name == "" {
		return nil, errors.New("Collection requires name to not be empty")
	}

	if desc.ID == 0 {
		return nil, errors.New("Collection ID must be greater than 0")
	}

	if !desc.Schema.IsEmpty() {
		if len(desc.Schema.Fields) == 0 {
			return nil, errors.New("Collection schema has no fields")
		}
		docKeyField := desc.Schema.Fields[0]
		if docKeyField.Kind != base.FieldKind_DocKey || docKeyField.Name != "_key" {
			return nil, errors.New("Collection schema first field must be a DocKey")
		}
		desc.Schema.FieldIDs = make([]uint32, len(desc.Schema.Fields))
		for i, field := range desc.Schema.Fields {
			if field.Name == "" {
				return nil, errors.New("Collection schema field missing Name")
			}
			if field.Kind == base.FieldKind_None {
				return nil, errors.New("Collection schema field missing FieldKind")
			}
			if (field.Kind != base.FieldKind_DocKey && !field.IsObject()) && field.Typ == core.NONE_CRDT {
				return nil, errors.New("Collection schema field missing CRDT type")
			}
			desc.Schema.FieldIDs[i] = uint32(i)
			desc.Schema.Fields[i].ID = base.FieldID(i)
		}
	}

	// for now, ignore any defined indexes, and overwrite the entire IndexDescription
	// property with the correct default one.
	desc.Indexes = []base.IndexDescription{
		{
			Name:    "primary",
			ID:      uint32(0),
			Primary: true,
			Unique:  true,
		},
	}

	return &Collection{
		db:        db,
		desc:      desc,
		colID:     desc.ID,
		colIDKey:  core.NewKey(fmt.Sprint(desc.ID)),
		hasSchema: !desc.Schema.IsEmpty(),
	}, nil
}

// CreateCollection creates a collection and saves it to the database in its system store.
// Note: Collection.ID is an autoincrementing value that is generated by the database.
func (db *DB) CreateCollection(desc base.CollectionDescription) (client.Collection, error) {
	// check if collection by this name exists
	key := base.MakeCollectionSystemKey(desc.Name)
	exists, err := db.systemstore.Has(key.ToDS())
	if err != nil {
		return nil, err
	}
	if exists {
		return nil, errors.New("Collection already exists")
	}

	colSeq, err := db.getSequence(collectionSeqKey)
	if err != nil {
		return nil, err
	}
	colID, err := colSeq.next()
	if err != nil {
		return nil, err
	}
	desc.ID = uint32(colID)
	col, err := db.newCollection(desc)
	if err != nil {
		return nil, err
	}

	buf, err := json.Marshal(col.desc)
	if err != nil {
		return nil, err
	}

	key = base.MakeCollectionSystemKey(col.desc.Name)

	//write the collection metadata to the system store
	err = db.systemstore.Put(key.ToDS(), buf)
	return col, err
}

// GetCollection returns an existing collection within the database
func (db *DB) GetCollection(name string) (client.Collection, error) {
	if name == "" {
		return nil, errors.New("Collection name can't be empty")
	}

	key := base.MakeCollectionSystemKey(name)
	buf, err := db.systemstore.Get(key.ToDS())
	if err != nil {
		return nil, err
	}

	var desc base.CollectionDescription
	err = json.Unmarshal(buf, &desc)
	if err != nil {
		return nil, err
	}

	return &Collection{
		db:        db,
		desc:      desc,
		colID:     desc.ID,
		colIDKey:  core.NewKey(fmt.Sprint(desc.ID)),
		hasSchema: !desc.Schema.IsEmpty(),
	}, nil
}

// ValidDescription
// func (c *Collection) ValidDescription() bool {
// 	return false
// }

// Description returns the base.CollectionDescription
func (c *Collection) Description() base.CollectionDescription {
	return c.desc
}

// Name returns the collection name
func (c *Collection) Name() string {
	return c.desc.Name
}

// Schema returns the Schema of the collection
func (c *Collection) Schema() base.SchemaDescription {
	return c.desc.Schema
}

// ID returns the ID of the collection
func (c *Collection) ID() uint32 {
	return c.colID
}

// Indexes returns the defined indexes on the Collection
// @todo: Properly handle index creation/management
func (c *Collection) Indexes() []base.IndexDescription {
	return c.desc.Indexes
}

// PrimaryIndex returns the primary index for the given collection
func (c *Collection) PrimaryIndex() base.IndexDescription {
	return c.desc.Indexes[0]
}

// Index returns the index with the given index ID
func (c *Collection) Index(id uint32) (base.IndexDescription, error) {
	for _, index := range c.desc.Indexes {
		if index.ID == id {
			return index, nil
		}
	}

	return base.IndexDescription{}, errors.New("No index found for given ID")
}

// CreateIndex creates a new index on the collection. Custom indexes
// are always "Secondary indexes". Primary indexes are automatically created
// on Collection creation, and cannot be changed.
func (c *Collection) CreateIndex(idesc base.IndexDescription) error {
	panic("not implemented")
}

// WithTxn returns a new instance of the collection, with a transaction
// handle instead of a raw DB handle
func (c *Collection) WithTxn(txn client.Txn) client.Collection {
	return &Collection{
		db:        c.db,
		txn:       txn.(*Txn),
		desc:      c.desc,
		colID:     c.colID,
		colIDKey:  c.colIDKey,
		hasSchema: c.hasSchema,
	}
}

// Create a new document
// Will verify the DocKey/CID to ensure that the new document is correctly formatted.
func (c *Collection) Create(doc *document.Document) error {
	txn, err := c.getTxn(false)
	if err != nil {
		return err
	}
	defer c.discardImplicitTxn(txn)

	err = c.create(txn, doc)
	if err != nil {
		return err
	}
	return c.commitImplicitTxn(txn)
}

// CreateMany creates a collection of documents at once.
// Will verify the DocKey/CID to ensure that the new documents are correctly formatted.
func (c *Collection) CreateMany(docs []*document.Document) error {
	txn, err := c.getTxn(false)
	if err != nil {
		return err
	}
	defer c.discardImplicitTxn(txn)

	for _, doc := range docs {
		err = c.create(txn, doc)
		if err != nil {
			return err
		}
	}
	return c.commitImplicitTxn(txn)
}

func (c *Collection) create(txn *Txn, doc *document.Document) error {
	// DocKey verification
	buf, err := doc.Bytes()
	if err != nil {
		return err
	}
	// @todo:  grab the cid Prefix from the DocKey internal CID if available
	pref := cid.Prefix{
		Version:  1,
		Codec:    cid.Raw,
		MhType:   mh.SHA2_256,
		MhLength: -1, // default length
	}
	// And then feed it some data
	doccid, err := pref.Sum(buf)
	if err != nil {
		return err
	}
	// fmt.Println(c)
	dockey := key.NewDocKeyV0(doccid)
	if !dockey.Key.Equal(doc.Key().Key) {
		return errors.Wrap(ErrDocVerification, fmt.Sprintf("Expected %s, got %s", doc.Key().UUID(), dockey.UUID()))
	}

	// check if doc already exists
	exists, err := c.exists(txn, doc.Key())
	if err != nil {
		return err
	}
	if exists {
		return ErrDocumentAlreadyExists
	}

	// write object marker
	err = writeObjectMarker(txn.datastore, c.getPrimaryIndexDocKey(doc.Key().Instance("v")))
	if err != nil {
		return err
	}
	// write data to DB via MerkleClock/CRDT
	return c.save(txn, doc)
}

// Update an existing document with the new values
// Any field that needs to be removed or cleared
// should call doc.Clear(field) before. Any field that
// is nil/empty that hasn't called Clear will be ignored
func (c *Collection) Update(doc *document.Document) error {
	txn, err := c.getTxn(false)
	if err != nil {
		return err
	}
	defer c.discardImplicitTxn(txn)

	exists, err := c.exists(txn, doc.Key())
	if err != nil {
		return err
	}
	if !exists {
		return ErrDocumentNotFound
	}

	err = c.update(txn, doc)
	if err != nil {
		return err
	}

	return c.commitImplicitTxn(txn)
}

// Contract: DB Exists check is already perfomed, and a doc with the given key exists
// Note: Should we CompareAndSet the update, IE: Query the state, and update if changed
// or, just update everything regardless.
// Should probably be smart about the update due to the MerkleCRDT overhead, shouldn't
// add to the bloat.
func (c *Collection) update(txn *Txn, doc *document.Document) error {
	err := c.save(txn, doc)
	if err != nil {
		return err
	}
	return nil
}

// Save a document into the db
// Either by creating a new document or by updating an existing one
func (c *Collection) Save(doc *document.Document) error {
	txn, err := c.getTxn(false)
	if err != nil {
		return err
	}
	defer c.discardImplicitTxn(txn)

	// Check if document already exists with key
	exists, err := c.exists(txn, doc.Key())
	if err != nil {
		return err
	}

	if exists {
		err = c.update(txn, doc)
	} else {
		err = c.create(txn, doc)
	}
	if err != nil {
		return err
	}
	return c.commitImplicitTxn(txn)
}

func (c *Collection) save(txn *Txn, doc *document.Document) error {
	// New batch transaction/store (optional/todo)
	// Ensute/Set doc object marker
	// Loop through doc values
	//	=> 		instanciate MerkleCRDT objects
	//	=> 		Set/Publish new CRDT values
	dockey := doc.Key().Key
	links := make([]core.DAGLink, 0)
	merge := make(map[string]interface{})
	for k, v := range doc.Fields() {
		val, _ := doc.GetValueWithField(v)
		if val.IsDirty() {
			fieldKey := c.getFieldKey(dockey, k)
			c, err := c.saveDocValue(txn, c.getPrimaryIndexDocKey(fieldKey), val)
			if err != nil {
				return err
			}
			if val.IsDelete() {
				doc.SetAs(v.Name(), nil, v.Type())
				merge[k] = nil
			} else {
				merge[k] = val.Value()
			}
			// set value as clean
			val.Clean()

			links = append(links, core.DAGLink{
				Name: k,
				Cid:  c,
			})
		}
	}
	// Update CompositeDAG
	em, err := cbor.CanonicalEncOptions().EncMode()
	if err != nil {
		return err
	}
	buf, err := em.Marshal(merge)
	if err != nil {
		return nil
	}

	_, err = c.saveValueToMerkleCRDT(txn, c.getPrimaryIndexDocKey(dockey), core.COMPOSITE, buf, links)
	return err
}

// Delete will attempt to delete a document by key
// will return true if a deltion is successful, and
// return false, along with an error, if it cannot.
// If the document doesn't exist, then it will return
// false, and a ErrDocumentNotFound error.
// This operation will all state relating to the given
// DocKey. This includes data, block, and head storage.
func (c *Collection) Delete(key key.DocKey) (bool, error) {
	// create txn
	txn, err := c.getTxn(false)
	if err != nil {
		return false, err
	}
	defer c.discardImplicitTxn(txn)

	exists, err := c.exists(txn, key)
	if err != nil {
		return false, err
	}
	if !exists {
		return false, ErrDocumentNotFound
	}

	// run delete, commit if successful
	deleted, err := c.delete(txn, key)
	if err != nil {
		return false, err
	}
	return deleted, c.commitImplicitTxn(txn)
}

// at the moment, delete only does data storage delete.
// Dag, and head store will soon follow.
func (c *Collection) delete(txn *Txn, key key.DocKey) (bool, error) {
	q := query.Query{
		Prefix:   c.getPrimaryIndexDocKey(key.Key).String(),
		KeysOnly: true,
	}
	res, err := txn.datastore.Query(q)

	for e := range res.Next() {
		if e.Error != nil {
			return false, err
		}

		err = txn.datastore.Delete(c.getPrimaryIndexDocKey(ds.NewKey(e.Key)))
		if err != nil {
			return false, err
		}
	}

	return true, nil
}

// Exists checks if a given document exists with supplied DocKey
func (c *Collection) Exists(key key.DocKey) (bool, error) {
	// create txn
	txn, err := c.getTxn(false)
	if err != nil {
		return false, err
	}
	defer c.discardImplicitTxn(txn)

	exists, err := c.exists(txn, key)
	if err != nil && err != ds.ErrNotFound {
		return false, err
	}
	return exists, c.commitImplicitTxn(txn)
}

// check if a document exists with the given key
func (c *Collection) exists(txn *Txn, key key.DocKey) (bool, error) {
	return txn.datastore.Has(c.getPrimaryIndexDocKey(key.Key.Instance("v")))
}

func (c *Collection) saveDocValue(txn *Txn, key ds.Key, val document.Value) (cid.Cid, error) {
	// datatype, err := c.db.crdtFactory.InstanceWithStores(txn, val.Type(), key)
	// if err != nil {
	// 	return cid.Cid{}, err
	// }
	switch val.Type() {
	case core.LWW_REGISTER:
		wval, ok := val.(document.WriteableValue)
		if !ok {
			return cid.Cid{}, document.ErrValueTypeMismatch
		}
		var bytes []byte
		var err error
		if val.IsDelete() { // empty byte array
			bytes = []byte{}
		} else {
			bytes, err = wval.Bytes()
			if err != nil {
				return cid.Cid{}, err
			}
		}
		return c.saveValueToMerkleCRDT(txn, key, core.LWW_REGISTER, bytes)
	default:
		return cid.Cid{}, ErrUnknownCRDT
	}
}

func (c *Collection) saveValueToMerkleCRDT(txn *Txn, key ds.Key, ctype core.CType, args ...interface{}) (cid.Cid, error) {
	switch ctype {
	case core.LWW_REGISTER:
		datatype, err := c.db.crdtFactory.InstanceWithStores(txn, ctype, key)
		if err != nil {
			return cid.Cid{}, err
		}

		var bytes []byte
		var ok bool
		// parse args
		if len(args) != 1 {
			return cid.Cid{}, ErrUnknownCRDTArgument
		}
		bytes, ok = args[0].([]byte)
		if !ok {
			return cid.Cid{}, ErrUnknownCRDTArgument
		}
		lwwreg := datatype.(*crdt.MerkleLWWRegister)
		return lwwreg.Set(bytes)
	case core.OBJECT:
		// db.writeObjectMarker(db.datastore, subdoc.Instance("v"))
		c.db.log.Debug("Sub objects not yet supported")
		break
	case core.COMPOSITE:
		key = key.ChildString("C") // @todo: Generalize COMPOSITE key suffix
		datatype, err := c.db.crdtFactory.InstanceWithStores(txn, ctype, key)
		if err != nil {
			return cid.Cid{}, err
		}
		var bytes []byte
		var links []core.DAGLink
		var ok bool
		// parse args
		if len(args) != 2 {
			return cid.Cid{}, ErrUnknownCRDTArgument
		}
		bytes, ok = args[0].([]byte)
		if !ok {
			return cid.Cid{}, ErrUnknownCRDTArgument
		}
		links, ok = args[1].([]core.DAGLink)
		if !ok {
			return cid.Cid{}, ErrUnknownCRDTArgument
		}
		comp := datatype.(*crdt.MerkleCompositeDAG)
		return comp.Set(bytes, nil, links) // @todo: add signature to compositeDAG SET op
	}
	return cid.Cid{}, ErrUnknownCRDT
}

// getTxn gets or creates a new transaction from the underlying db.
// If the collection already has a txn, return the existing one.
// Otherwise, create a new implicit transaction.
func (c *Collection) getTxn(readonly bool) (*Txn, error) {
	if c.txn != nil {
		return c.txn, nil
	}
	return c.db.newTxn(readonly)
}

// discardImplicitTxn is a proxy function used by the collection to execute the Discard() transaction
// function only if its an implicit transaction.
// Implicit transactions are transactions that are created *during* an operation execution as a side effect.
// Explicit transactions are provided to the collection object via the "WithTxn(...)" function.
func (c *Collection) discardImplicitTxn(txn *Txn) {
	if c.txn == nil {
		txn.Discard()
	}
}

func (c *Collection) commitImplicitTxn(txn *Txn) error {
	if c.txn == nil {
		return txn.Commit()
	}
	return nil
}

func (c *Collection) GetIndexDocKey(key ds.Key, indexID uint32) ds.Key {
	return c.GetIndexDocKey(key, indexID)
}

func (c *Collection) getIndexDocKey(key ds.Key, indexID uint32) ds.Key {
	return c.colIDKey.ChildString(fmt.Sprint(indexID)).Child(key)
}

func (c *Collection) GetPrimaryIndexDocKey(key ds.Key) ds.Key {
	return c.getPrimaryIndexDocKey(key)
}

func (c *Collection) getPrimaryIndexDocKey(key ds.Key) ds.Key {
	return c.getIndexDocKey(key, c.PrimaryIndex().ID)
}

func (c *Collection) getFieldKey(key ds.Key, fieldName string) ds.Key {
	if c.hasSchema {
		return key.ChildString(fmt.Sprint(c.getSchemaFieldID(fieldName)))
	}
	return key.ChildString(fieldName)
}

// getSchemaFieldID returns the FieldID of the given fieldName.
// It assumes a schema exists for the collection, and that the
// field exists in the schema.
func (c *Collection) getSchemaFieldID(fieldName string) uint32 {
	for _, field := range c.desc.Schema.Fields {
		if field.Name == fieldName {
			return uint32(field.ID)
		}
	}
	return uint32(0)
}

func writeObjectMarker(store ds.Write, key ds.Key) error {
	if key.Name() != "v" {
		key = key.Instance("v")
	}
	return store.Put(key, []byte{base.ObjectMarker})
}

// makeCollectionKey returns a formatted collection key for the system data store.
// it assumes the name of the collection is non-empty.
// func makeCollectionDataKey(collectionID uint32) ds.Key {
// 	return collectionNs.ChildString(name)
// }
