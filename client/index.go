// Copyright 2023 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package client

// IndexDirection is the direction of an index.
type IndexDirection string

const (
	// Ascending is the value to use for an ascending fields
	Ascending IndexDirection = "ASC"
	// Descending is the value to use for an descending fields
	Descending IndexDirection = "DESC"
)

// IndexFieldDescription describes how a field is being indexed.
type IndexedFieldDescription struct {
	// Name contains the name of the field.
	Name string
	// Direction contains the direction of the index.
	Direction IndexDirection
}

// IndexDescription describes an index.
type IndexDescription struct {
	// Name contains the name of the index.
	Name string
	// ID is the local identifier of this index.
	ID uint32
	// Fields contains the fields that are being indexed.
	Fields []IndexedFieldDescription
}

// CollectIndexedFields returns all fields that are indexed by all collection indexes.
func (d CollectionDescription) CollectIndexedFields() []FieldDescription {
	fieldsMap := make(map[string]FieldDescription)
	for _, index := range d.Indexes {
		for _, field := range index.Fields {
			for i := range d.Schema.Fields {
				colField := d.Schema.Fields[i]
				if field.Name == colField.Name {
					fieldsMap[field.Name] = colField
					break
				}
			}
		}
	}
	fields := make([]FieldDescription, 0, len(fieldsMap))
	for _, field := range fieldsMap {
		fields = append(fields, field)
	}
	return fields
}
