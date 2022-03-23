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
	"context"

	"github.com/sourcenetwork/defradb/datastore"

	"github.com/sourcenetwork/defradb/core"
)

// Fetcher is the interface for collecting documents
// from the underlying data store. It handles all
// the key/value scanning, aggregation, and document
// encoding.
type Fetcher interface {
	Start(ctx context.Context, txn datastore.Txn, spans core.Spans) error
	FetchNextMap(ctx context.Context) ([]byte, map[string]interface{}, error)
	Close() error
}
