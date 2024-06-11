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

// AVG 用于求平均值，通过sum/count求得。
// AVG 我们并不能预期在不同的数据库上，精度会不会损失，以及损失的话会有多少的损失。这很大程度上跟数据库类型，数据库驱动实现都有关
type AVG struct {
	name            string
	avgColumnInfo   merger.ColumnInfo
	sumColumnInfo   merger.ColumnInfo
	countColumnInfo merger.ColumnInfo
}

// NewAVG sumInfo是sum的信息，countInfo是count的信息，avgName用于Column方法
func NewAVG(avgInfo, sumInfo, countInfo merger.ColumnInfo) *AVG {
	return &AVG{
		name:            "AVG",
		avgColumnInfo:   avgInfo,
		sumColumnInfo:   sumInfo,
		countColumnInfo: countInfo,
	}
}

func (a *AVG) Aggregate(cols [][]any) (any, error) {
	// cols[0] 代表第一个sql.Rows，用于确定avgFunc
	avgFunc, err := a.findAvgFunc(cols[0])
	if err != nil {
		return nil, err
	}
	return avgFunc(cols, a.sumColumnInfo.Index, a.countColumnInfo.Index)
}

func (a *AVG) findAvgFunc(col []any) (func([][]any, int, int) (float64, error), error) {
	sumIndex := a.sumColumnInfo.Index
	countIndex := a.countColumnInfo.Index
	if sumIndex >= len(col) || sumIndex < 0 || countIndex >= len(col) || countIndex < 0 {
		return nil, errs.ErrMergerInvalidAggregateColumnIndex
	}
	sumKind := reflect.TypeOf(col[sumIndex]).Kind()
	countKind := reflect.TypeOf(col[countIndex]).Kind()
	val, ok := avgAggregateFuncMapping[[2]reflect.Kind{sumKind, countKind}]
	if !ok {
		return nil, errs.ErrMergerAggregateFuncNotFound
	}
	return val, nil
}

func (a *AVG) ColumnInfo() merger.ColumnInfo {
	return a.avgColumnInfo
}

func (a *AVG) Name() string {
	return a.name
}

// avgAggregator cols就是上面Aggregate的入参cols可以参Aggregate的描述
func avgAggregator[S AggregateElement, C AggregateElement](cols [][]any, sumIndex int, countIndex int) (float64, error) {
	var sum S
	var count C
	for _, col := range cols {
		sum += col[sumIndex].(S)
		count += col[countIndex].(C)
	}
	val := float64(sum) / float64(count)
	return val, nil

}

var avgAggregateFuncMapping = map[[2]reflect.Kind]func([][]any, int, int) (float64, error){
	[2]reflect.Kind{reflect.Int, reflect.Int}:     avgAggregator[int, int],
	[2]reflect.Kind{reflect.Int, reflect.Int8}:    avgAggregator[int, int8],
	[2]reflect.Kind{reflect.Int, reflect.Int16}:   avgAggregator[int, int16],
	[2]reflect.Kind{reflect.Int, reflect.Int32}:   avgAggregator[int, int32],
	[2]reflect.Kind{reflect.Int, reflect.Int64}:   avgAggregator[int, int64],
	[2]reflect.Kind{reflect.Int, reflect.Uint}:    avgAggregator[int, uint],
	[2]reflect.Kind{reflect.Int, reflect.Uint8}:   avgAggregator[int, uint8],
	[2]reflect.Kind{reflect.Int, reflect.Uint16}:  avgAggregator[int, uint16],
	[2]reflect.Kind{reflect.Int, reflect.Uint32}:  avgAggregator[int, uint32],
	[2]reflect.Kind{reflect.Int, reflect.Uint64}:  avgAggregator[int, uint64],
	[2]reflect.Kind{reflect.Int, reflect.Float32}: avgAggregator[int, float32],
	[2]reflect.Kind{reflect.Int, reflect.Float64}: avgAggregator[int, float64],

	[2]reflect.Kind{reflect.Int8, reflect.Int}:     avgAggregator[int8, int],
	[2]reflect.Kind{reflect.Int8, reflect.Int8}:    avgAggregator[int8, int8],
	[2]reflect.Kind{reflect.Int8, reflect.Int16}:   avgAggregator[int8, int16],
	[2]reflect.Kind{reflect.Int8, reflect.Int32}:   avgAggregator[int8, int32],
	[2]reflect.Kind{reflect.Int8, reflect.Int64}:   avgAggregator[int8, int64],
	[2]reflect.Kind{reflect.Int8, reflect.Uint}:    avgAggregator[int8, uint],
	[2]reflect.Kind{reflect.Int8, reflect.Uint8}:   avgAggregator[int8, uint8],
	[2]reflect.Kind{reflect.Int8, reflect.Uint16}:  avgAggregator[int8, uint16],
	[2]reflect.Kind{reflect.Int8, reflect.Uint32}:  avgAggregator[int8, uint32],
	[2]reflect.Kind{reflect.Int8, reflect.Uint64}:  avgAggregator[int8, uint64],
	[2]reflect.Kind{reflect.Int8, reflect.Float32}: avgAggregator[int8, float32],
	[2]reflect.Kind{reflect.Int8, reflect.Float64}: avgAggregator[int8, float64],

	[2]reflect.Kind{reflect.Int16, reflect.Int}:     avgAggregator[int16, int],
	[2]reflect.Kind{reflect.Int16, reflect.Int8}:    avgAggregator[int16, int8],
	[2]reflect.Kind{reflect.Int16, reflect.Int16}:   avgAggregator[int16, int16],
	[2]reflect.Kind{reflect.Int16, reflect.Int32}:   avgAggregator[int16, int32],
	[2]reflect.Kind{reflect.Int16, reflect.Int64}:   avgAggregator[int16, int64],
	[2]reflect.Kind{reflect.Int16, reflect.Uint}:    avgAggregator[int16, uint],
	[2]reflect.Kind{reflect.Int16, reflect.Uint8}:   avgAggregator[int16, uint8],
	[2]reflect.Kind{reflect.Int16, reflect.Uint16}:  avgAggregator[int16, uint16],
	[2]reflect.Kind{reflect.Int16, reflect.Uint32}:  avgAggregator[int16, uint32],
	[2]reflect.Kind{reflect.Int16, reflect.Uint64}:  avgAggregator[int16, uint64],
	[2]reflect.Kind{reflect.Int16, reflect.Float32}: avgAggregator[int16, float32],
	[2]reflect.Kind{reflect.Int16, reflect.Float64}: avgAggregator[int16, float64],

	[2]reflect.Kind{reflect.Int32, reflect.Int}:     avgAggregator[int16, int],
	[2]reflect.Kind{reflect.Int32, reflect.Int8}:    avgAggregator[int16, int8],
	[2]reflect.Kind{reflect.Int32, reflect.Int16}:   avgAggregator[int16, int16],
	[2]reflect.Kind{reflect.Int32, reflect.Int32}:   avgAggregator[int16, int32],
	[2]reflect.Kind{reflect.Int32, reflect.Int64}:   avgAggregator[int16, int64],
	[2]reflect.Kind{reflect.Int32, reflect.Uint}:    avgAggregator[int16, uint],
	[2]reflect.Kind{reflect.Int32, reflect.Uint8}:   avgAggregator[int16, uint8],
	[2]reflect.Kind{reflect.Int32, reflect.Uint16}:  avgAggregator[int16, uint16],
	[2]reflect.Kind{reflect.Int32, reflect.Uint32}:  avgAggregator[int16, uint32],
	[2]reflect.Kind{reflect.Int32, reflect.Uint64}:  avgAggregator[int16, uint64],
	[2]reflect.Kind{reflect.Int32, reflect.Float32}: avgAggregator[int16, float32],
	[2]reflect.Kind{reflect.Int32, reflect.Float64}: avgAggregator[int16, float64],

	[2]reflect.Kind{reflect.Int64, reflect.Int}:     avgAggregator[int64, int],
	[2]reflect.Kind{reflect.Int64, reflect.Int8}:    avgAggregator[int64, int8],
	[2]reflect.Kind{reflect.Int64, reflect.Int16}:   avgAggregator[int64, int16],
	[2]reflect.Kind{reflect.Int64, reflect.Int32}:   avgAggregator[int64, int32],
	[2]reflect.Kind{reflect.Int64, reflect.Int64}:   avgAggregator[int64, int64],
	[2]reflect.Kind{reflect.Int64, reflect.Uint}:    avgAggregator[int64, uint],
	[2]reflect.Kind{reflect.Int64, reflect.Uint8}:   avgAggregator[int64, uint8],
	[2]reflect.Kind{reflect.Int64, reflect.Uint16}:  avgAggregator[int64, uint16],
	[2]reflect.Kind{reflect.Int64, reflect.Uint32}:  avgAggregator[int64, uint32],
	[2]reflect.Kind{reflect.Int64, reflect.Uint64}:  avgAggregator[int64, uint64],
	[2]reflect.Kind{reflect.Int64, reflect.Float32}: avgAggregator[int64, float32],
	[2]reflect.Kind{reflect.Int64, reflect.Float64}: avgAggregator[int64, float64],

	[2]reflect.Kind{reflect.Uint, reflect.Int}:     avgAggregator[uint, int],
	[2]reflect.Kind{reflect.Uint, reflect.Int8}:    avgAggregator[uint, int8],
	[2]reflect.Kind{reflect.Uint, reflect.Int16}:   avgAggregator[uint, int16],
	[2]reflect.Kind{reflect.Uint, reflect.Int32}:   avgAggregator[uint, int32],
	[2]reflect.Kind{reflect.Uint, reflect.Int64}:   avgAggregator[uint, int64],
	[2]reflect.Kind{reflect.Uint, reflect.Uint}:    avgAggregator[uint, uint],
	[2]reflect.Kind{reflect.Uint, reflect.Uint8}:   avgAggregator[uint, uint8],
	[2]reflect.Kind{reflect.Uint, reflect.Uint16}:  avgAggregator[uint, uint16],
	[2]reflect.Kind{reflect.Uint, reflect.Uint32}:  avgAggregator[uint, uint32],
	[2]reflect.Kind{reflect.Uint, reflect.Uint64}:  avgAggregator[uint, uint64],
	[2]reflect.Kind{reflect.Uint, reflect.Float32}: avgAggregator[uint, float32],
	[2]reflect.Kind{reflect.Uint, reflect.Float64}: avgAggregator[uint, float64],

	[2]reflect.Kind{reflect.Uint8, reflect.Int}:     avgAggregator[uint8, int],
	[2]reflect.Kind{reflect.Uint8, reflect.Int8}:    avgAggregator[uint8, int8],
	[2]reflect.Kind{reflect.Uint8, reflect.Int16}:   avgAggregator[uint8, int16],
	[2]reflect.Kind{reflect.Uint8, reflect.Int32}:   avgAggregator[uint8, int32],
	[2]reflect.Kind{reflect.Uint8, reflect.Int64}:   avgAggregator[uint8, int64],
	[2]reflect.Kind{reflect.Uint8, reflect.Uint}:    avgAggregator[uint8, uint],
	[2]reflect.Kind{reflect.Uint8, reflect.Uint8}:   avgAggregator[uint8, uint8],
	[2]reflect.Kind{reflect.Uint8, reflect.Uint16}:  avgAggregator[uint8, uint16],
	[2]reflect.Kind{reflect.Uint8, reflect.Uint32}:  avgAggregator[uint8, uint32],
	[2]reflect.Kind{reflect.Uint8, reflect.Uint64}:  avgAggregator[uint8, uint64],
	[2]reflect.Kind{reflect.Uint8, reflect.Float32}: avgAggregator[uint8, float32],
	[2]reflect.Kind{reflect.Uint8, reflect.Float64}: avgAggregator[uint8, float64],

	[2]reflect.Kind{reflect.Uint16, reflect.Int}:     avgAggregator[uint16, int],
	[2]reflect.Kind{reflect.Uint16, reflect.Int8}:    avgAggregator[uint16, int8],
	[2]reflect.Kind{reflect.Uint16, reflect.Int16}:   avgAggregator[uint16, int16],
	[2]reflect.Kind{reflect.Uint16, reflect.Int32}:   avgAggregator[uint16, int32],
	[2]reflect.Kind{reflect.Uint16, reflect.Int64}:   avgAggregator[uint16, int64],
	[2]reflect.Kind{reflect.Uint16, reflect.Uint}:    avgAggregator[uint16, uint],
	[2]reflect.Kind{reflect.Uint16, reflect.Uint8}:   avgAggregator[uint16, uint8],
	[2]reflect.Kind{reflect.Uint16, reflect.Uint16}:  avgAggregator[uint16, uint16],
	[2]reflect.Kind{reflect.Uint16, reflect.Uint32}:  avgAggregator[uint16, uint32],
	[2]reflect.Kind{reflect.Uint16, reflect.Uint64}:  avgAggregator[uint16, uint64],
	[2]reflect.Kind{reflect.Uint16, reflect.Float32}: avgAggregator[uint16, float32],
	[2]reflect.Kind{reflect.Uint16, reflect.Float64}: avgAggregator[uint16, float64],

	[2]reflect.Kind{reflect.Uint32, reflect.Int}:     avgAggregator[uint32, int],
	[2]reflect.Kind{reflect.Uint32, reflect.Int8}:    avgAggregator[uint32, int8],
	[2]reflect.Kind{reflect.Uint32, reflect.Int16}:   avgAggregator[uint32, int16],
	[2]reflect.Kind{reflect.Uint32, reflect.Int32}:   avgAggregator[uint32, int32],
	[2]reflect.Kind{reflect.Uint32, reflect.Int64}:   avgAggregator[uint32, int64],
	[2]reflect.Kind{reflect.Uint32, reflect.Uint}:    avgAggregator[uint32, uint],
	[2]reflect.Kind{reflect.Uint32, reflect.Uint8}:   avgAggregator[uint32, uint8],
	[2]reflect.Kind{reflect.Uint32, reflect.Uint16}:  avgAggregator[uint32, uint16],
	[2]reflect.Kind{reflect.Uint32, reflect.Uint32}:  avgAggregator[uint32, uint32],
	[2]reflect.Kind{reflect.Uint32, reflect.Uint64}:  avgAggregator[uint32, uint64],
	[2]reflect.Kind{reflect.Uint32, reflect.Float32}: avgAggregator[uint32, float32],
	[2]reflect.Kind{reflect.Uint32, reflect.Float64}: avgAggregator[uint32, float64],

	[2]reflect.Kind{reflect.Uint64, reflect.Int}:     avgAggregator[uint64, int],
	[2]reflect.Kind{reflect.Uint64, reflect.Int8}:    avgAggregator[uint64, int8],
	[2]reflect.Kind{reflect.Uint64, reflect.Int16}:   avgAggregator[uint64, int16],
	[2]reflect.Kind{reflect.Uint64, reflect.Int32}:   avgAggregator[uint64, int32],
	[2]reflect.Kind{reflect.Uint64, reflect.Int64}:   avgAggregator[uint64, int64],
	[2]reflect.Kind{reflect.Uint64, reflect.Uint}:    avgAggregator[uint64, uint],
	[2]reflect.Kind{reflect.Uint64, reflect.Uint8}:   avgAggregator[uint64, uint8],
	[2]reflect.Kind{reflect.Uint64, reflect.Uint16}:  avgAggregator[uint64, uint16],
	[2]reflect.Kind{reflect.Uint64, reflect.Uint32}:  avgAggregator[uint64, uint32],
	[2]reflect.Kind{reflect.Uint64, reflect.Uint64}:  avgAggregator[uint64, uint64],
	[2]reflect.Kind{reflect.Uint64, reflect.Float32}: avgAggregator[uint64, float32],
	[2]reflect.Kind{reflect.Uint64, reflect.Float64}: avgAggregator[uint64, float64],

	[2]reflect.Kind{reflect.Float32, reflect.Int}:     avgAggregator[float32, int],
	[2]reflect.Kind{reflect.Float32, reflect.Int8}:    avgAggregator[float32, int8],
	[2]reflect.Kind{reflect.Float32, reflect.Int16}:   avgAggregator[float32, int16],
	[2]reflect.Kind{reflect.Float32, reflect.Int32}:   avgAggregator[float32, int32],
	[2]reflect.Kind{reflect.Float32, reflect.Int64}:   avgAggregator[float32, int64],
	[2]reflect.Kind{reflect.Float32, reflect.Uint}:    avgAggregator[float32, uint],
	[2]reflect.Kind{reflect.Float32, reflect.Uint8}:   avgAggregator[float32, uint8],
	[2]reflect.Kind{reflect.Float32, reflect.Uint16}:  avgAggregator[float32, uint16],
	[2]reflect.Kind{reflect.Float32, reflect.Uint32}:  avgAggregator[float32, uint32],
	[2]reflect.Kind{reflect.Float32, reflect.Uint64}:  avgAggregator[float32, uint64],
	[2]reflect.Kind{reflect.Float32, reflect.Float32}: avgAggregator[float32, float32],
	[2]reflect.Kind{reflect.Float32, reflect.Float64}: avgAggregator[float32, float64],

	[2]reflect.Kind{reflect.Float64, reflect.Int}:     avgAggregator[float64, int],
	[2]reflect.Kind{reflect.Float64, reflect.Int8}:    avgAggregator[float64, int8],
	[2]reflect.Kind{reflect.Float64, reflect.Int16}:   avgAggregator[float64, int16],
	[2]reflect.Kind{reflect.Float64, reflect.Int32}:   avgAggregator[float64, int32],
	[2]reflect.Kind{reflect.Float64, reflect.Int64}:   avgAggregator[float64, int64],
	[2]reflect.Kind{reflect.Float64, reflect.Uint}:    avgAggregator[float64, uint],
	[2]reflect.Kind{reflect.Float64, reflect.Uint8}:   avgAggregator[float64, uint8],
	[2]reflect.Kind{reflect.Float64, reflect.Uint16}:  avgAggregator[float64, uint16],
	[2]reflect.Kind{reflect.Float64, reflect.Uint32}:  avgAggregator[float64, uint32],
	[2]reflect.Kind{reflect.Float64, reflect.Uint64}:  avgAggregator[float64, uint64],
	[2]reflect.Kind{reflect.Float64, reflect.Float32}: avgAggregator[float64, float32],
	[2]reflect.Kind{reflect.Float64, reflect.Float64}: avgAggregator[float64, float64],
}
