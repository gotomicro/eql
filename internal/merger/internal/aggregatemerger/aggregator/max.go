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

type Max struct {
	maxColumnInfo merger.ColumnInfo
}

func (m *Max) Aggregate(cols [][]any) (any, error) {
	maxFunc, err := m.findMaxFunc(cols[0])
	if err != nil {
		return nil, err
	}
	return maxFunc(cols, m.maxColumnInfo.Index)
}
func (m *Max) findMaxFunc(col []any) (func([][]any, int) (any, error), error) {
	var kind reflect.Kind
	maxIndex := m.maxColumnInfo.Index
	if maxIndex < 0 || maxIndex >= len(col) {
		return nil, errs.ErrMergerInvalidAggregateColumnIndex
	}
	kind = reflect.TypeOf(col[maxIndex]).Kind()
	countFunc, ok := maxFuncMapping[kind]
	if !ok {
		return nil, errs.ErrMergerAggregateFuncNotFound
	}
	return countFunc, nil
}

func (m *Max) ColumnName() string {
	return m.maxColumnInfo.SelectName()
}

func NewMax(info merger.ColumnInfo) *Max {
	return &Max{
		maxColumnInfo: info,
	}
}

func maxAggregator[T AggregateElement](colsData [][]any, maxIndex int) (any, error) {
	return findExtremeValue[T](colsData, isMaxValue[T], maxIndex)
}

var maxFuncMapping = map[reflect.Kind]func([][]any, int) (any, error){
	reflect.Int:     maxAggregator[int],
	reflect.Int8:    maxAggregator[int8],
	reflect.Int16:   maxAggregator[int16],
	reflect.Int32:   maxAggregator[int32],
	reflect.Int64:   maxAggregator[int64],
	reflect.Uint8:   maxAggregator[uint8],
	reflect.Uint16:  maxAggregator[uint16],
	reflect.Uint32:  maxAggregator[uint32],
	reflect.Uint64:  maxAggregator[uint64],
	reflect.Float32: maxAggregator[float32],
	reflect.Float64: maxAggregator[float64],
	reflect.Uint:    maxAggregator[uint],
}

type extremeValueFunc[T AggregateElement] func(T, T) bool

func findExtremeValue[T AggregateElement](colsData [][]any, isExtremeValue extremeValueFunc[T], index int) (any, error) {
	var ans T
	for idx, colData := range colsData {
		data := colData[index].(T)
		if idx == 0 {
			ans = data
			continue
		}
		if isExtremeValue(ans, data) {
			ans = data
		}
	}
	return ans, nil
}

func isMaxValue[T AggregateElement](maxData T, data T) bool {
	return maxData < data
}
