// Copyright 2021 ecodeclub
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aggregator

import (
	"reflect"

	"github.com/ecodeclub/eorm/internal/merger"

	"github.com/ecodeclub/eorm/internal/merger/internal/errs"
)

type Count struct {
	countInfo merger.ColumnInfo
}

func (s *Count) Aggregate(cols [][]any) (any, error) {
	countFunc, err := s.findCountFunc(cols[0])
	if err != nil {
		return nil, err
	}
	return countFunc(cols, s.countInfo.Index)
}
func (s *Count) findCountFunc(col []any) (func([][]any, int) (any, error), error) {
	var kind reflect.Kind
	countIndex := s.countInfo.Index
	if countIndex < 0 || countIndex >= len(col) {
		return nil, errs.ErrMergerInvalidAggregateColumnIndex
	}
	kind = reflect.TypeOf(col[countIndex]).Kind()
	countFunc, ok := countAggregateFuncMapping[kind]
	if !ok {
		return nil, errs.ErrMergerAggregateFuncNotFound
	}
	return countFunc, nil
}

func (s *Count) ColumnName() string {
	return s.countInfo.SelectName()
}

func NewCount(info merger.ColumnInfo) *Count {
	return &Count{
		countInfo: info,
	}
}

func countAggregate[T AggregateElement](cols [][]any, countIndex int) (any, error) {
	var count T
	for _, col := range cols {
		count += col[countIndex].(T)
	}
	return count, nil
}

var countAggregateFuncMapping = map[reflect.Kind]func([][]any, int) (any, error){
	reflect.Int:     countAggregate[int],
	reflect.Int8:    countAggregate[int8],
	reflect.Int16:   countAggregate[int16],
	reflect.Int32:   countAggregate[int32],
	reflect.Int64:   countAggregate[int64],
	reflect.Uint8:   countAggregate[uint8],
	reflect.Uint16:  countAggregate[uint16],
	reflect.Uint32:  countAggregate[uint32],
	reflect.Uint64:  countAggregate[uint64],
	reflect.Float32: countAggregate[float32],
	reflect.Float64: countAggregate[float64],
	reflect.Uint:    countAggregate[uint],
}
