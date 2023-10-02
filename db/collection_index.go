// Copyright 2023 Democratized Data Foundation
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
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/core"
	"github.com/sourcenetwork/defradb/datastore"
	"github.com/sourcenetwork/defradb/db/base"
	"github.com/sourcenetwork/defradb/db/fetcher"
	"github.com/sourcenetwork/defradb/request/graphql/schema"
)

// createCollectionIndex creates a new collection index and saves it to the database in its system store.
func (db *db) createCollectionIndex(
	ctx context.Context,
	txn datastore.Txn,
	collectionName string,
	desc client.IndexDescription,
) (client.IndexDescription, error) {
	col, err := db.getCollectionByName(ctx, txn, collectionName)
	if err != nil {
		return client.IndexDescription{}, NewErrCanNotReadCollection(collectionName, err)
	}
	col = col.WithTxn(txn)
	return col.CreateIndex(ctx, desc)
}

func (db *db) dropCollectionIndex(
	ctx context.Context,
	txn datastore.Txn,
	collectionName, indexName string,
) error {
	col, err := db.getCollectionByName(ctx, txn, collectionName)
	if err != nil {
		return NewErrCanNotReadCollection(collectionName, err)
	}
	col = col.WithTxn(txn)
	return col.DropIndex(ctx, indexName)
}

// getAllIndexes returns all the indexes in the database.
func (db *db) getAllIndexes(
	ctx context.Context,
	txn datastore.Txn,
) (map[client.CollectionName][]client.IndexDescription, error) {
	prefix := core.NewCollectionIndexKey("", "")

	keys, indexDescriptions, err := datastore.DeserializePrefix[client.IndexDescription](ctx,
		prefix.ToString(), txn.Systemstore())

	if err != nil {
		return nil, err
	}

	indexes := make(map[client.CollectionName][]client.IndexDescription)

	for i := range keys {
		indexKey, err := core.NewCollectionIndexKeyFromString(keys[i])
		if err != nil {
			return nil, NewErrInvalidStoredIndexKey(indexKey.ToString())
		}
		indexes[indexKey.CollectionName] = append(
			indexes[indexKey.CollectionName],
			indexDescriptions[i],
		)
	}

	return indexes, nil
}

func (db *db) fetchCollectionIndexDescriptions(
	ctx context.Context,
	txn datastore.Txn,
	colName string,
) ([]client.IndexDescription, error) {
	prefix := core.NewCollectionIndexKey(colName, "")
	_, indexDescriptions, err := datastore.DeserializePrefix[client.IndexDescription](ctx,
		prefix.ToString(), txn.Systemstore())
	if err != nil {
		return nil, err
	}
	return indexDescriptions, nil
}

func (c *collection) indexNewDoc(ctx context.Context, txn datastore.Txn, doc *client.Document) error {
	err := c.loadIndexes(ctx, txn)
	if err != nil {
		return err
	}
	for _, index := range c.indexes {
		err = index.Save(ctx, txn, doc)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *collection) updateIndexedDoc(
	ctx context.Context,
	txn datastore.Txn,
	doc *client.Document,
) error {
	err := c.loadIndexes(ctx, txn)
	if err != nil {
		return err
	}
	oldDoc, err := c.get(
		ctx,
		txn,
		c.getPrimaryKeyFromDocKey(doc.Key()), c.Description().CollectIndexedFields(),
		false,
	)
	if err != nil {
		return err
	}
	for _, index := range c.indexes {
		err = index.Update(ctx, txn, oldDoc, doc)
		if err != nil {
			return err
		}
	}
	return nil
}

// CreateIndex creates a new index on the collection.
//
// If the index name is empty, a name will be automatically generated.
// Otherwise its uniqueness will be checked against existing indexes and
// it will be validated with `schema.IsValidIndexName` method.
//
// The provided index description must include at least one field with
// a name that exists in the collection schema.
// Also it's `ID` field must be zero. It will be assigned a unique
// incremental value by the database.
//
// The index description will be stored in the system store.
//
// Once finished, if there are existing documents in the collection,
// the documents will be indexed by the new index.
func (c *collection) CreateIndex(
	ctx context.Context,
	desc client.IndexDescription,
) (client.IndexDescription, error) {
	txn, err := c.getTxn(ctx, false)
	if err != nil {
		return client.IndexDescription{}, err
	}
	defer c.discardImplicitTxn(ctx, txn)

	index, err := c.createIndex(ctx, txn, desc)
	if err != nil {
		return client.IndexDescription{}, err
	}
	return index.Description(), c.commitImplicitTxn(ctx, txn)
}

func (c *collection) createIndex(
	ctx context.Context,
	txn datastore.Txn,
	desc client.IndexDescription,
) (CollectionIndex, error) {
	if desc.Name != "" && !schema.IsValidIndexName(desc.Name) {
		return nil, schema.NewErrIndexWithInvalidName("!")
	}
	err := validateIndexDescription(desc)
	if err != nil {
		return nil, err
	}

	err = c.checkExistingFields(ctx, desc.Fields)
	if err != nil {
		return nil, err
	}

	indexKey, err := c.generateIndexNameIfNeededAndCreateKey(ctx, txn, &desc)
	if err != nil {
		return nil, err
	}

	colSeq, err := c.db.getSequence(ctx, txn, fmt.Sprintf("%s/%d", core.COLLECTION_INDEX, c.ID()))
	if err != nil {
		return nil, err
	}
	colID, err := colSeq.next(ctx, txn)
	if err != nil {
		return nil, err
	}
	desc.ID = uint32(colID)

	buf, err := json.Marshal(desc)
	if err != nil {
		return nil, err
	}

	err = txn.Systemstore().Put(ctx, indexKey.ToDS(), buf)
	if err != nil {
		return nil, err
	}
	colIndex, err := NewCollectionIndex(c, desc)
	if err != nil {
		return nil, err
	}
	c.desc.Indexes = append(c.desc.Indexes, colIndex.Description())
	c.indexes = append(c.indexes, colIndex)
	err = c.indexExistingDocs(ctx, txn, colIndex)
	if err != nil {
		return nil, err
	}
	return colIndex, nil
}

func (c *collection) iterateAllDocs(
	ctx context.Context,
	txn datastore.Txn,
	fields []client.FieldDescription,
	exec func(doc *client.Document) error,
) error {
	df := c.newFetcher()
	err := df.Init(ctx, txn, &c.desc, fields, nil, nil, false, false)
	if err != nil {
		_ = df.Close()
		return err
	}
	start := base.MakeCollectionKey(c.desc)
	spans := core.NewSpans(core.NewSpan(start, start.PrefixEnd()))

	err = df.Start(ctx, spans)
	if err != nil {
		_ = df.Close()
		return err
	}

	for {
		encodedDoc, _, err := df.FetchNext(ctx)
		if err != nil {
			_ = df.Close()
			return err
		}
		if encodedDoc == nil {
			break
		}

		doc, err := fetcher.Decode(encodedDoc)
		if err != nil {
			return err
		}

		err = exec(doc)
		if err != nil {
			return err
		}
	}

	return df.Close()
}

func (c *collection) indexExistingDocs(
	ctx context.Context,
	txn datastore.Txn,
	index CollectionIndex,
) error {
	fields := make([]client.FieldDescription, 0, 1)
	for _, field := range index.Description().Fields {
		for i := range c.desc.Schema.Fields {
			colField := c.desc.Schema.Fields[i]
			if field.Name == colField.Name {
				fields = append(fields, colField)
				break
			}
		}
	}

	return c.iterateAllDocs(ctx, txn, fields, func(doc *client.Document) error {
		return index.Save(ctx, txn, doc)
	})
}

// DropIndex removes an index from the collection.
//
// The index will be removed from the system store.
//
// All index artifacts for existing documents related the index will be removed.
func (c *collection) DropIndex(ctx context.Context, indexName string) error {
	txn, err := c.getTxn(ctx, false)
	if err != nil {
		return err
	}
	defer c.discardImplicitTxn(ctx, txn)

	err = c.dropIndex(ctx, txn, indexName)
	if err != nil {
		return err
	}
	return c.commitImplicitTxn(ctx, txn)
}

func (c *collection) dropIndex(ctx context.Context, txn datastore.Txn, indexName string) error {
	err := c.loadIndexes(ctx, txn)
	if err != nil {
		return err
	}

	var didFind bool
	for i := range c.indexes {
		if c.indexes[i].Name() == indexName {
			err = c.indexes[i].RemoveAll(ctx, txn)
			if err != nil {
				return err
			}
			c.indexes = append(c.indexes[:i], c.indexes[i+1:]...)
			didFind = true
			break
		}
	}
	if !didFind {
		return NewErrIndexWithNameDoesNotExists(indexName)
	}

	for i := range c.desc.Indexes {
		if c.desc.Indexes[i].Name == indexName {
			c.desc.Indexes = append(c.desc.Indexes[:i], c.desc.Indexes[i+1:]...)
			break
		}
	}
	key := core.NewCollectionIndexKey(c.Name(), indexName)
	err = txn.Systemstore().Delete(ctx, key.ToDS())
	if err != nil {
		return err
	}

	return nil
}

func (c *collection) dropAllIndexes(ctx context.Context, txn datastore.Txn) error {
	prefix := core.NewCollectionIndexKey(c.Name(), "")

	keys, err := datastore.FetchKeysForPrefix(ctx, prefix.ToString(), txn.Systemstore())
	if err != nil {
		return err
	}

	for _, key := range keys {
		err = txn.Systemstore().Delete(ctx, key)
		if err != nil {
			return err
		}
	}

	return err
}

func (c *collection) loadIndexes(ctx context.Context, txn datastore.Txn) error {
	indexDescriptions, err := c.db.fetchCollectionIndexDescriptions(ctx, txn, c.Name())
	if err != nil {
		return err
	}
	colIndexes := make([]CollectionIndex, 0, len(indexDescriptions))
	for _, indexDesc := range indexDescriptions {
		index, err := NewCollectionIndex(c, indexDesc)
		if err != nil {
			return err
		}
		colIndexes = append(colIndexes, index)
	}
	c.desc.Indexes = indexDescriptions
	c.indexes = colIndexes
	return nil
}

// GetIndexes returns all indexes for the collection.
func (c *collection) GetIndexes(ctx context.Context) ([]client.IndexDescription, error) {
	txn, err := c.getTxn(ctx, false)
	if err != nil {
		return nil, err
	}
	defer c.discardImplicitTxn(ctx, txn)

	err = c.loadIndexes(ctx, txn)
	if err != nil {
		return nil, err
	}
	return c.desc.Indexes, nil
}

func (c *collection) checkExistingFields(
	ctx context.Context,
	fields []client.IndexedFieldDescription,
) error {
	collectionFields := c.Description().Schema.Fields
	for _, field := range fields {
		found := false
		for _, colField := range collectionFields {
			if field.Name == colField.Name {
				found = true
				break
			}
		}
		if !found {
			return NewErrNonExistingFieldForIndex(field.Name)
		}
	}
	return nil
}

func (c *collection) generateIndexNameIfNeededAndCreateKey(
	ctx context.Context,
	txn datastore.Txn,
	desc *client.IndexDescription,
) (core.CollectionIndexKey, error) {
	var indexKey core.CollectionIndexKey
	if desc.Name == "" {
		nameIncrement := 1
		for {
			desc.Name = generateIndexName(c, desc.Fields, nameIncrement)
			indexKey = core.NewCollectionIndexKey(c.Name(), desc.Name)
			exists, err := txn.Systemstore().Has(ctx, indexKey.ToDS())
			if err != nil {
				return core.CollectionIndexKey{}, err
			}
			if !exists {
				break
			}
			nameIncrement++
		}
	} else {
		indexKey = core.NewCollectionIndexKey(c.Name(), desc.Name)
		exists, err := txn.Systemstore().Has(ctx, indexKey.ToDS())
		if err != nil {
			return core.CollectionIndexKey{}, err
		}
		if exists {
			return core.CollectionIndexKey{}, NewErrIndexWithNameAlreadyExists(desc.Name)
		}
	}
	return indexKey, nil
}

func validateIndexDescription(desc client.IndexDescription) error {
	if desc.ID != 0 {
		return NewErrNonZeroIndexIDProvided(desc.ID)
	}
	if len(desc.Fields) == 0 {
		return ErrIndexMissingFields
	}
	if len(desc.Fields) == 1 && desc.Fields[0].Direction == client.Descending {
		return ErrIndexSingleFieldWrongDirection
	}
	for i := range desc.Fields {
		if desc.Fields[i].Name == "" {
			return ErrIndexFieldMissingName
		}
		if desc.Fields[i].Direction == "" {
			desc.Fields[i].Direction = client.Ascending
		}
	}
	return nil
}

func generateIndexName(col client.Collection, fields []client.IndexedFieldDescription, inc int) string {
	sb := strings.Builder{}
	// at the moment we support only single field indexes that can be stored only in
	// ascending order. This will change once we introduce composite indexes.
	direction := "ASC"
	sb.WriteString(col.Name())
	sb.WriteByte('_')
	// we can safely assume that there is at least one field in the slice
	// because we validate it before calling this function
	sb.WriteString(fields[0].Name)
	sb.WriteByte('_')
	sb.WriteString(direction)
	if inc > 1 {
		sb.WriteByte('_')
		sb.WriteString(strconv.Itoa(inc))
	}
	return sb.String()
}
