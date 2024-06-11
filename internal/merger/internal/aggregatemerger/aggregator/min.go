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

type Min struct {
	name          string
	minColumnInfo merger.ColumnInfo
}

func (m *Min) Aggregate(cols [][]any) (any, error) {
	minFunc, err := m.findMinFunc(cols[0])
	if err != nil {
		return nil, err
	}
	return minFunc(cols, m.minColumnInfo.Index)
}

func (m *Min) findMinFunc(col []any) (func([][]any, int) (any, error), error) {
	var kind reflect.Kind
	minIndex := m.minColumnInfo.Index
	if minIndex < 0 || minIndex >= len(col) {
		return nil, errs.ErrMergerInvalidAggregateColumnIndex
	}
	kind = reflect.TypeOf(col[minIndex]).Kind()
	minFunc, ok := minFuncMapping[kind]
	if !ok {
		return nil, errs.ErrMergerAggregateFuncNotFound
	}
	return minFunc, nil
}

func (m *Min) ColumnInfo() merger.ColumnInfo {
	return m.minColumnInfo
}

func (m *Min) Name() string {
	return m.name
}

func NewMin(info merger.ColumnInfo) *Min {
	return &Min{
		name:          "MIN",
		minColumnInfo: info,
	}
}

func minAggregator[T AggregateElement](colsData [][]any, minIndex int) (any, error) {
	return findExtremeValue[T](colsData, isMinValue[T], minIndex)
}

var minFuncMapping = map[reflect.Kind]func([][]any, int) (any, error){
	reflect.Int:     minAggregator[int],
	reflect.Int8:    minAggregator[int8],
	reflect.Int16:   minAggregator[int16],
	reflect.Int32:   minAggregator[int32],
	reflect.Int64:   minAggregator[int64],
	reflect.Uint8:   minAggregator[uint8],
	reflect.Uint16:  minAggregator[uint16],
	reflect.Uint32:  minAggregator[uint32],
	reflect.Uint64:  minAggregator[uint64],
	reflect.Float32: minAggregator[float32],
	reflect.Float64: minAggregator[float64],
	reflect.Uint:    minAggregator[uint],
}

func isMinValue[T AggregateElement](minData T, data T) bool {
	return minData > data
}
