// Copyright 2022 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package fetcher

import (
	"bytes"
	"context"
	"errors"

	dsq "github.com/ipfs/go-datastore/query"
	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/datastore"
	"github.com/sourcenetwork/defradb/datastore/iterable"

	"github.com/sourcenetwork/defradb/core"
	"github.com/sourcenetwork/defradb/db/base"
)

var (
	_ Fetcher = (*PrimaryIndexFetcher)(nil)
)

type PrimaryIndexFetcher struct {
	col *client.CollectionDescription

	txn          datastore.Txn
	spans        core.Spans
	curSpanIndex int

	schemaFields map[uint32]client.FieldDescription

	doc        *encodedDocument
	decodedDoc *client.Document

	kv                *core.KeyValue
	kvIter            iterable.Iterator
	kvResultsIter     dsq.Results
	readComplete      bool
	isReadingDocument bool
}

// Init implements PrimaryIndexFetcher
func (df *PrimaryIndexFetcher) Init(col *client.CollectionDescription) error {
	if col.IsEmpty() {
		return errors.New("PrimaryIndexFetcher must be given a schema")
	}

	df.col = col
	df.isReadingDocument = false
	df.doc = new(encodedDocument)

	if df.kvResultsIter != nil {
		if err := df.kvResultsIter.Close(); err != nil {
			return err
		}
	}
	df.kvResultsIter = nil
	if df.kvIter != nil {
		if err := df.kvIter.Close(); err != nil {
			return err
		}
	}
	df.kvIter = nil

	df.schemaFields = make(map[uint32]client.FieldDescription)
	for _, field := range col.Schema.Fields {
		df.schemaFields[uint32(field.ID)] = field
	}
	return nil
}

// Start implements PrimaryIndexFetcher
func (df *PrimaryIndexFetcher) Start(ctx context.Context, txn datastore.Txn, spans core.Spans) error {
	if df.col == nil {
		return errors.New("PrimaryIndexFetcher cannot be started without a CollectionDescription")
	}
	if df.doc == nil {
		return errors.New("PrimaryIndexFetcher cannot be started without an initialized document object")
	}
	//@todo: Handle fields Description
	// check spans
	numspans := len(spans)
	var uniqueSpans core.Spans
	if numspans == 0 { // no specified spans so create a prefix scan key for the entire collection/index
		primaryIndex := df.col.GetPrimaryIndex()
		start := base.MakeIndexPrefixKey(*df.col, &primaryIndex)
		uniqueSpans = core.Spans{core.NewSpan(start, start.PrefixEnd())}
	} else {
		uniqueSpans = spans.MergeAscending()
	}

	df.spans = uniqueSpans
	df.curSpanIndex = -1
	df.txn = txn

	return df.startNextSpan(ctx)
}

func (df *PrimaryIndexFetcher) startNextSpan(ctx context.Context) error {
	nextSpanIndex := df.curSpanIndex + 1
	if nextSpanIndex >= len(df.spans) {
		return nil
	}

	var err error
	if df.kvIter == nil {
		df.kvIter, err = df.txn.Datastore().GetIterator(dsq.Query{
			Orders: []dsq.Order{dsq.OrderByKey{}},
		})
	}
	if err != nil {
		return err
	}

	if df.kvResultsIter != nil {
		err = df.kvResultsIter.Close()
		if err != nil {
			return err
		}
	}

	span := df.spans[nextSpanIndex]
	df.kvResultsIter, err = df.kvIter.IteratePrefix(ctx, span.Start().ToDS(), span.End().ToDS())
	if err != nil {
		return err
	}
	df.curSpanIndex = nextSpanIndex

	_, err = df.nextKey(ctx)
	return err
}

// nextKey gets the next kv. It sets both kv and kvEnd internally.
// It returns true if the current doc is completed
func (df *PrimaryIndexFetcher) nextKey(ctx context.Context) (docDone bool, err error) {
	for {
		res, available := df.kvResultsIter.NextSync()
		if res.Error != nil {
			return true, res.Error
		}

		df.readComplete = !available
		if !available {
			err := df.startNextSpan(ctx)
			if err != nil {
				return false, err
			}
			return true, nil
		}

		df.kv = &core.KeyValue{
			Key:   core.NewDataStoreKey(res.Key),
			Value: res.Value,
		}

		// skip if we are iterating through a non value kv pair
		if df.kv.Key.InstanceType != "v" {
			continue
		}

		// skip object markers
		if bytes.Equal(df.kv.Value, []byte{base.ObjectMarker}) {
			continue
		}

		// check if we've crossed document boundries
		if df.doc.Key != nil && df.kv.Key.DocKey != string(df.doc.Key) {
			df.isReadingDocument = false
			return true, nil
		}
		return false, nil
	}
}

// processKV continuously processes the key value pairs we've received
// and step by step constructs the current encoded document
func (df *PrimaryIndexFetcher) processKV(kv *core.KeyValue) error {
	// skip MerkleCRDT meta-data priority key-value pair
	// implement here <--
	// instance := kv.Key.Name()
	// if instance != "v" {
	// 	return nil
	// }
	if df.doc == nil {
		return errors.New("Failed to process KV, uninitialized document object")
	}

	if !df.isReadingDocument {
		df.isReadingDocument = true
		df.doc.Reset()
		df.doc.Key = []byte(kv.Key.DocKey)
	}

	// extract the FieldID and update the encoded doc properties map
	fieldID, err := kv.Key.FieldID()
	if err != nil {
		return err
	}
	fieldDesc, exists := df.schemaFields[fieldID]
	if !exists {
		return errors.New("Found field with no matching FieldDescription")
	}

	// @todo: Secondary Index might not have encoded FieldIDs
	// @body: Need to generalized the processKV, and overall Fetcher architecture
	// to better handle dynamic use cases beyond primary indexes. If a
	// secondary index is provided, we need to extract the indexed/implicit fields
	// from the KV pair.
	df.doc.Properties[fieldDesc] = &encProperty{
		Desc: fieldDesc,
		Raw:  kv.Value,
	}
	// @todo: Extract Index implicit/stored keys
	return nil
}

// FetchNext returns a raw binary encoded document. It iterates over all the relevant
// keypairs from the underlying store and constructs the document.
func (df *PrimaryIndexFetcher) FetchNext(ctx context.Context) (*encodedDocument, error) {
	if df.readComplete {
		return nil, nil
	}

	if df.kv == nil {
		return nil, errors.New("Failed to get document, fetcher hasn't been initalized or started")
	}
	// save the DocKey of the current kv pair so we can track when we cross the doc pair boundries
	// keyparts := df.kv.Key.List()
	// key := keyparts[len(keyparts)-2]

	// iterate until we have collected all the necessary kv pairs for the doc
	// we'll know when were done when either
	// A) Reach the end of the iterator
	for {
		err := df.processKV(df.kv)
		if err != nil {
			return nil, err
		}

		end, err := df.nextKey(ctx)
		if err != nil {
			return nil, err
		}
		if end {
			return df.doc, nil
		}

		// // crossed document kv boundary?
		// // if so, return document
		// newkeyparts := df.kv.Key.List()
		// newKey := newkeyparts[len(newkeyparts)-2]
		// if newKey != key {
		// 	return df.doc, nil
		// }
	}
}

// FetchNextDecoded implements PrimaryIndexFetcher
func (df *PrimaryIndexFetcher) FetchNextDecoded(ctx context.Context) (*client.Document, error) {
	encdoc, err := df.FetchNext(ctx)
	if err != nil {
		return nil, err
	}
	if encdoc == nil {
		return nil, nil
	}

	df.decodedDoc, err = encdoc.Decode()
	if err != nil {
		return nil, err
	}

	return df.decodedDoc, nil
}

// FetchNextMap returns the next document as a map[string]interface{}
// The first return value is the parsed document key
func (df *PrimaryIndexFetcher) FetchNextMap(ctx context.Context) ([]byte, map[string]interface{}, error) {
	encdoc, err := df.FetchNext(ctx)
	if err != nil {
		return nil, nil, err
	}
	if encdoc == nil {
		return nil, nil, nil
	}

	doc, err := encdoc.DecodeToMap()
	if err != nil {
		return nil, nil, err
	}
	return encdoc.Key, doc, err
}

func (df *PrimaryIndexFetcher) Close() error {
	if df.kvIter == nil {
		return nil
	}

	err := df.kvIter.Close()
	if err != nil {
		return err
	}

	if df.kvResultsIter == nil {
		return nil
	}

	return df.kvResultsIter.Close()
}
