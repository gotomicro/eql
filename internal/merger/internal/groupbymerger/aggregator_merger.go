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

package groupbymerger

import (
	"context"
	"database/sql"
	"reflect"
	"sync"
	_ "unsafe"

	"github.com/ecodeclub/eorm/internal/rows"

	"github.com/ecodeclub/ekit/slice"

	"github.com/ecodeclub/ekit/sqlx"

	"go.uber.org/multierr"

	"github.com/ecodeclub/ekit/mapx"
	"github.com/ecodeclub/eorm/internal/merger"
	"github.com/ecodeclub/eorm/internal/merger/internal/aggregatemerger/aggregator"
	"github.com/ecodeclub/eorm/internal/merger/internal/errs"
)

type AggregatorMerger struct {
	aggregators  []aggregator.Aggregator
	groupColumns []merger.ColumnInfo
	columnsName  []string
}

func NewAggregatorMerger(aggregators []aggregator.Aggregator, groupColumns []merger.ColumnInfo) *AggregatorMerger {
	cols := make([]string, 0, len(aggregators)+len(groupColumns))
	for _, groubyCol := range groupColumns {
		cols = append(cols, groubyCol.SelectName())
	}
	for _, agg := range aggregators {
		cols = append(cols, agg.ColumnName())
	}

	return &AggregatorMerger{
		aggregators:  aggregators,
		groupColumns: groupColumns,
		columnsName:  cols,
	}
}

// Merge 该实现会全部拿取results里面的数据，由于sql.Rows数据拿完之后会自动关闭，所以这边隐式的关闭了所有的sql.Rows
func (a *AggregatorMerger) Merge(ctx context.Context, results []rows.Rows) (rows.Rows, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if len(results) == 0 {
		return nil, errs.ErrMergerEmptyRows
	}

	if slice.Contains[rows.Rows](results, nil) {
		return nil, errs.ErrMergerRowsIsNull
	}
	// TODO: 无奈之举, 下方getCols会ScanAll然后出问题, 需要写测试覆盖
	columnTypes, err := results[0].ColumnTypes()
	if err != nil {
		return nil, err
	}
	dataMap, dataIndex, err := a.getCols(results)
	if err != nil {
		return nil, err
	}

	return &AggregatorRows{
		rowsList:     results,
		columnTypes:  columnTypes,
		aggregators:  a.aggregators,
		groupColumns: a.groupColumns,
		mu:           &sync.RWMutex{},
		dataMap:      dataMap,
		dataIndex:    dataIndex,
		cur:          -1,
		cols:         a.columnsName,
	}, nil
}

func (a *AggregatorMerger) getCols(rowsList []rows.Rows) (*mapx.TreeMap[Key, [][]any], []Key, error) {
	treeMap, err := mapx.NewTreeMap[Key, [][]any](compareKey)
	if err != nil {
		return nil, nil, err
	}
	keys := make([]Key, 0, 16)
	for _, res := range rowsList {
		scanner, err := sqlx.NewSQLRowsScanner(res)
		if err != nil {
			return nil, nil, err
		}
		colsData, err := scanner.ScanAll()
		if err != nil {
			return nil, nil, err
		}
		for _, colData := range colsData {
			key := Key{columnValues: make([]any, 0, len(a.groupColumns))}
			for _, groupByCol := range a.groupColumns {
				key.columnValues = append(key.columnValues, colData[groupByCol.Index])
			}
			val, ok := treeMap.Get(key)
			if ok {
				val = append(val, colData)
				err = treeMap.Put(key, val)
				if err != nil {
					return nil, nil, err
				}
			} else {
				keys = append(keys, key)
				err := treeMap.Put(key, [][]any{colData})
				if err != nil {
					return nil, nil, err
				}
			}
		}
	}
	return treeMap, keys, nil
}

type AggregatorRows struct {
	rowsList     []rows.Rows
	columnTypes  []*sql.ColumnType
	aggregators  []aggregator.Aggregator
	groupColumns []merger.ColumnInfo
	dataMap      *mapx.TreeMap[Key, [][]any]
	cur          int
	dataIndex    []Key
	mu           *sync.RWMutex
	curData      []any
	closed       bool
	lastErr      error
	cols         []string
}

func (a *AggregatorRows) ColumnTypes() ([]*sql.ColumnType, error) {
	// TODO: 这里是为了让测试通过的临时处理方法,貌似merger会先将
	//       正常应该先判断closed是否为true, 然后再a.rowsList[0].ColumnTypes()
	return a.columnTypes, nil
}

func (*AggregatorRows) NextResultSet() bool {
	return false
}

// Next 返回列的顺序先分组信息然后是聚合函数信息
func (a *AggregatorRows) Next() bool {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return false
	}
	a.cur++
	if a.cur >= len(a.dataIndex) {
		a.mu.Unlock()
		_ = a.Close()
		return false
	}
	a.curData = make([]any, 0, len(a.aggregators)+len(a.groupColumns))

	a.curData = append(a.curData, a.dataIndex[a.cur].columnValues...)

	for _, agg := range a.aggregators {
		val, _ := a.dataMap.Get(a.dataIndex[a.cur])
		res, err := agg.Aggregate(val)
		if err != nil {
			a.lastErr = err
			a.mu.Unlock()
			_ = a.Close()
			return false
		}
		a.curData = append(a.curData, res)
	}

	a.mu.Unlock()
	return true
}

func (a *AggregatorRows) Scan(dest ...any) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.lastErr != nil {
		return a.lastErr
	}
	if a.closed {
		return errs.ErrMergerRowsClosed
	}
	if a.cur == -1 {
		return errs.ErrMergerScanNotNext
	}
	for i := 0; i < len(dest); i++ {
		err := rows.ConvertAssign(dest[i], a.curData[i])
		if err != nil {
			return err
		}
	}
	return nil
}

// Close 关闭所有的sql.Rows
func (a *AggregatorRows) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.closed = true
	errorList := make([]error, 0, len(a.rowsList))
	for i := 0; i < len(a.rowsList); i++ {
		row := a.rowsList[i]
		err := row.Close()
		if err != nil {
			errorList = append(errorList, err)
		}
	}
	return multierr.Combine(errorList...)
}

// Columns 返回列的顺序先分组信息然后是聚合函数信息
func (a *AggregatorRows) Columns() ([]string, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.closed {
		return nil, errs.ErrMergerRowsClosed
	}
	return a.cols, nil
}

func (a *AggregatorRows) Err() error {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.lastErr
}

type Key struct {
	columnValues []any
}

func compareKey(a, b Key) int {
	keyLen := len(a.columnValues)
	for i := 0; i < keyLen; i++ {
		compareFunc := compareFuncMapping[reflect.TypeOf(a.columnValues[i]).Kind()]
		res := compareFunc(a.columnValues[i], b.columnValues[i])
		if res != 0 {
			return res
		}
	}
	return 0
}

func compare[T Ordered](ii any, jj any) int {
	i, j := ii.(T), jj.(T)
	if i < j {
		return -1
	} else if i > j {
		return 1
	} else {
		return 0
	}
}

type Ordered interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~float32 | ~float64 | ~string
}

var compareFuncMapping = map[reflect.Kind]func(any, any) int{
	reflect.Int:     compare[int],
	reflect.Int8:    compare[int8],
	reflect.Int16:   compare[int16],
	reflect.Int32:   compare[int32],
	reflect.Int64:   compare[int64],
	reflect.Uint8:   compare[uint8],
	reflect.Uint16:  compare[uint16],
	reflect.Uint32:  compare[uint32],
	reflect.Uint64:  compare[uint64],
	reflect.Float32: compare[float32],
	reflect.Float64: compare[float64],
	reflect.String:  compare[string],
	reflect.Uint:    compare[uint],
	reflect.Bool:    compareBool,
}

func compareBool(ii, jj any) int {
	i, j := ii.(bool), jj.(bool)
	if i == j {
		return 0
	}
	if i && !j {
		return 1
	}
	return -1
}
