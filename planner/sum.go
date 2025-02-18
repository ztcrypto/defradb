// Copyright 2022 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package planner

import (
	"github.com/sourcenetwork/immutable"
	"github.com/sourcenetwork/immutable/enumerable"

	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/client/request"
	"github.com/sourcenetwork/defradb/core"
	"github.com/sourcenetwork/defradb/planner/mapper"
)

type sumNode struct {
	documentIterator
	docMapper

	p    *Planner
	plan planNode

	isFloat           bool
	virtualFieldIndex int
	aggregateMapping  []mapper.AggregateTarget

	execInfo sumExecInfo
}

type sumExecInfo struct {
	// Total number of times sumNode was executed.
	iterations uint64
}

func (p *Planner) Sum(
	field *mapper.Aggregate,
	parent *mapper.Select,
) (*sumNode, error) {
	isFloat := false
	for _, target := range field.AggregateTargets {
		isTargetFloat, err := p.isValueFloat(parent, &target)
		if err != nil {
			return nil, err
		}
		// If one source property is a float, the result will be a float - no need to check the rest
		if isTargetFloat {
			isFloat = true
			break
		}
	}

	return &sumNode{
		p:                 p,
		isFloat:           isFloat,
		aggregateMapping:  field.AggregateTargets,
		virtualFieldIndex: field.Index,
		docMapper:         docMapper{field.DocumentMapping},
	}, nil
}

// Returns true if the value to be summed is a float, otherwise false.
func (p *Planner) isValueFloat(
	parent *mapper.Select,
	source *mapper.AggregateTarget,
) (bool, error) {
	// It is important that averages are floats even if their underlying values are ints
	// else sum will round them down to the nearest whole number
	if source.ChildTarget.Name == request.AverageFieldName {
		return true, nil
	}

	if !source.ChildTarget.HasValue {
		parentDescription, err := p.getCollectionDesc(parent.CollectionName)
		if err != nil {
			return false, err
		}

		fieldDescription, fieldDescriptionFound := parentDescription.Schema.GetField(source.Name)
		if !fieldDescriptionFound {
			return false, client.NewErrFieldNotExist(source.Name)
		}
		return fieldDescription.Kind == client.FieldKind_FLOAT_ARRAY ||
			fieldDescription.Kind == client.FieldKind_FLOAT ||
			fieldDescription.Kind == client.FieldKind_NILLABLE_FLOAT_ARRAY, nil
	}

	// If path length is two, we are summing a group or a child relationship
	if source.ChildTarget.Name == request.CountFieldName {
		// If we are summing a count, we know it is an int and can return false early
		return false, nil
	}

	child, isChildSelect := parent.FieldAt(source.Index).AsSelect()
	if !isChildSelect {
		return false, ErrMissingChildSelect
	}

	if _, isAggregate := request.Aggregates[source.ChildTarget.Name]; isAggregate {
		// If we are aggregating an aggregate, we need to traverse the aggregation chain down to
		// the root field in order to determine the value type.  This is recursive to allow handling
		// of N-depth aggregations (e.g. sum of sum of sum of...)
		sourceField := child.FieldAt(source.ChildTarget.Index).(*mapper.Aggregate)

		for _, aggregateTarget := range sourceField.AggregateTargets {
			isFloat, err := p.isValueFloat(
				child,
				&aggregateTarget,
			)
			if err != nil {
				return false, err
			}

			// If one source property is a float, the result will be a float - no need to check the rest
			if isFloat {
				return true, nil
			}
		}
		return false, nil
	}

	childCollectionDescription, err := p.getCollectionDesc(child.CollectionName)
	if err != nil {
		return false, err
	}

	fieldDescription, fieldDescriptionFound := childCollectionDescription.Schema.GetField(source.ChildTarget.Name)
	if !fieldDescriptionFound {
		return false, client.NewErrFieldNotExist(source.ChildTarget.Name)
	}

	return fieldDescription.Kind == client.FieldKind_FLOAT_ARRAY ||
		fieldDescription.Kind == client.FieldKind_FLOAT ||
		fieldDescription.Kind == client.FieldKind_NILLABLE_FLOAT_ARRAY, nil
}

func (n *sumNode) Kind() string {
	return "sumNode"
}

func (n *sumNode) Init() error {
	return n.plan.Init()
}

func (n *sumNode) Start() error { return n.plan.Start() }

func (n *sumNode) Spans(spans core.Spans) { n.plan.Spans(spans) }

func (n *sumNode) Close() error { return n.plan.Close() }

func (n *sumNode) Source() planNode { return n.plan }

func (n *sumNode) simpleExplain() (map[string]any, error) {
	sourceExplanations := make([]map[string]any, len(n.aggregateMapping))

	for i, source := range n.aggregateMapping {
		simpleExplainMap := map[string]any{}

		// Add the filter attribute if it exists.
		if source.Filter == nil {
			simpleExplainMap[filterLabel] = nil
		} else {
			// get the target aggregate document mapping. Since the filters
			// are relative to the target aggregate collection (and doc mapper).
			var targetMap *core.DocumentMapping
			if source.Index < len(n.documentMapping.ChildMappings) &&
				n.documentMapping.ChildMappings[source.Index] != nil {
				targetMap = n.documentMapping.ChildMappings[source.Index]
			} else {
				targetMap = n.documentMapping
			}
			simpleExplainMap[filterLabel] = source.Filter.ToMap(targetMap)
		}

		// Add the main field name.
		simpleExplainMap[fieldNameLabel] = source.Field.Name

		// Add the child field name if it exists.
		if source.ChildTarget.HasValue {
			simpleExplainMap[childFieldNameLabel] = source.ChildTarget.Name
		} else {
			simpleExplainMap[childFieldNameLabel] = nil
		}

		sourceExplanations[i] = simpleExplainMap
	}

	return map[string]any{
		sourcesLabel: sourceExplanations,
	}, nil
}

// Explain method returns a map containing all attributes of this node that
// are to be explained, subscribes / opts-in this node to be an explainablePlanNode.
func (n *sumNode) Explain(explainType request.ExplainType) (map[string]any, error) {
	switch explainType {
	case request.SimpleExplain:
		return n.simpleExplain()

	case request.ExecuteExplain:
		return map[string]any{
			"iterations": n.execInfo.iterations,
		}, nil

	default:
		return nil, ErrUnknownExplainRequestType
	}
}

func (n *sumNode) Next() (bool, error) {
	n.execInfo.iterations++

	hasNext, err := n.plan.Next()
	if err != nil || !hasNext {
		return hasNext, err
	}

	n.currentValue = n.plan.Value()

	sum := float64(0)

	for _, source := range n.aggregateMapping {
		child := n.currentValue.Fields[source.Index]
		var collectionSum float64
		var err error
		switch childCollection := child.(type) {
		case []core.Doc:
			collectionSum = sumDocs(childCollection, func(childItem core.Doc) float64 {
				childProperty := childItem.Fields[source.ChildTarget.Index]
				switch v := childProperty.(type) {
				case int:
					return float64(v)
				case int64:
					return float64(v)
				case uint64:
					return float64(v)
				case float64:
					return v
				default:
					// return nothing, cannot be summed
					return 0
				}
			})
		case []int64:
			collectionSum, err = sumItems(
				childCollection,
				&source,
				lessN[int64],
				func(childItem int64) float64 {
					return float64(childItem)
				},
			)

		case []immutable.Option[int64]:
			collectionSum, err = sumItems(
				childCollection,
				&source,
				lessO[int64],
				func(childItem immutable.Option[int64]) float64 {
					if !childItem.HasValue() {
						return 0
					}
					return float64(childItem.Value())
				},
			)

		case []float64:
			collectionSum, err = sumItems(
				childCollection,
				&source,
				lessN[float64],
				func(childItem float64) float64 {
					return childItem
				},
			)

		case []immutable.Option[float64]:
			collectionSum, err = sumItems(
				childCollection,
				&source,
				lessO[float64],
				func(childItem immutable.Option[float64]) float64 {
					if !childItem.HasValue() {
						return 0
					}
					return childItem.Value()
				},
			)
		}
		if err != nil {
			return false, err
		}
		sum += collectionSum
	}

	var typedSum any
	if n.isFloat {
		typedSum = sum
	} else {
		typedSum = int64(sum)
	}
	n.currentValue.Fields[n.virtualFieldIndex] = typedSum

	return true, nil
}

// offsets sums the documents in a slice, skipping over hidden items (a grouping mechanic).
// Docs should be counted with this function to avoid applying offsets twice (once in the
// select, then once here).
func sumDocs(docs []core.Doc, toFloat func(core.Doc) float64) float64 {
	var sum float64 = 0
	for _, doc := range docs {
		if !doc.Hidden {
			sum += toFloat(doc)
		}
	}

	return sum
}

func sumItems[T any](
	source []T,
	aggregateTarget *mapper.AggregateTarget,
	less func(T, T) bool,
	toFloat func(T) float64,
) (float64, error) {
	items := enumerable.New(source)
	if aggregateTarget.Filter != nil {
		items = enumerable.Where(items, func(item T) (bool, error) {
			return mapper.RunFilter(item, aggregateTarget.Filter)
		})
	}

	if aggregateTarget.OrderBy != nil && len(aggregateTarget.OrderBy.Conditions) > 0 {
		if aggregateTarget.OrderBy.Conditions[0].Direction == mapper.ASC {
			items = enumerable.Sort(items, less, len(source))
		} else {
			items = enumerable.Sort(items, reverse(less), len(source))
		}
	}

	if aggregateTarget.Limit != nil {
		items = enumerable.Skip(items, aggregateTarget.Limit.Offset)
		items = enumerable.Take(items, aggregateTarget.Limit.Limit)
	}

	var sum float64 = 0
	err := enumerable.ForEach(items, func(item T) {
		sum += toFloat(item)
	})

	return sum, err
}

func (n *sumNode) SetPlan(p planNode) { n.plan = p }

type number interface {
	int64 | float64
}

func lessN[T number](a T, b T) bool {
	return a < b
}

func lessO[T number](a immutable.Option[T], b immutable.Option[T]) bool {
	if !a.HasValue() {
		return true
	}

	if !b.HasValue() {
		return false
	}

	return a.Value() < b.Value()
}

func reverse[T any](original func(T, T) bool) func(T, T) bool {
	return func(t1, t2 T) bool {
		return !original(t1, t2)
	}
}
