// Copyright 2022 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package indexed

import (
	"testing"

	testUtils "github.com/sourcenetwork/defradb/db/tests"
)

func TestQueryIndexedWithNumberIndexEqualsFilterBlock(t *testing.T) {
	tests := []testUtils.QueryTestCase{
		{
			Description: "Indexed query with non-matching equals filter",
			Query: `query {
						users(filter: {Age: {_eq: 22}}) {
							Name
							Age
						}
					}`,
			Docs: map[int][]string{
				0: {
					(`{
					"Name": "John",
					"Age": 21
				}`)},
			},
			Results: []map[string]interface{}{},
		},
		{
			Description: "Indexed query with matching equals filter",
			Query: `query {
						users(filter: {Age: {_eq: 21}}) {
							Name
							Age
						}
					}`,
			Docs: map[int][]string{
				0: {
					(`{
					"Name": "John",
					"Age": 21
				}`)},
			},
			Results: []map[string]interface{}{
				{
					"Name": "John",
					"Age":  uint64(21),
				},
			},
		},
	}

	for _, test := range tests {
		executeTestCase(t, test)
	}
}
