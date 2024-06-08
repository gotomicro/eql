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

package sortmerger

import (
	"container/heap"
	"context"
	"database/sql"
	"fmt"
	"log"
	"reflect"
	"sync"

	"github.com/ecodeclub/eorm/internal/rows"

	"go.uber.org/multierr"

	"github.com/ecodeclub/eorm/internal/merger/internal/errs"
)

type Order bool

const (
	// ASC 升序排序
	ASC Order = true
	// DESC 降序排序
	DESC Order = false
)

type Ordered interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~float32 | ~float64 | ~string
}

type SortColumn struct {
	name  string
	order Order
}

func NewSortColumn(colName string, order Order) SortColumn {
	return SortColumn{
		name:  colName,
		order: order,
	}
}

type sortColumns struct {
	columns []SortColumn
	colMap  map[string]int
}

func (s sortColumns) Has(name string) bool {
	_, ok := s.colMap[name]
	return ok
}

func (s sortColumns) Find(name string) int {
	return s.colMap[name]
}

func (s sortColumns) Get(index int) SortColumn {
	return s.columns[index]
}

func (s sortColumns) Len() int {
	return len(s.columns)
}

// Merger  如果有GroupBy子句，会导致排序是给每个分组排的，那么该实现无法运作正常
type Merger struct {
	sortColumns
	cols       []string
	preScanAll bool
}

// NewMerger preScanAll 表示是否预先扫描出结果集中的所有到内存
func NewMerger(preScanAll bool, sortCols ...SortColumn) (*Merger, error) {
	scs, err := newSortColumns(sortCols...)
	if err != nil {
		return nil, err
	}
	return &Merger{
		preScanAll:  preScanAll,
		sortColumns: scs,
	}, nil
}

func newSortColumns(sortCols ...SortColumn) (sortColumns, error) {
	if len(sortCols) == 0 {
		return sortColumns{}, errs.ErrEmptySortColumns
	}
	sortMap := make(map[string]int, len(sortCols))
	for idx, sortCol := range sortCols {
		if _, ok := sortMap[sortCol.name]; ok {
			return sortColumns{}, errs.NewRepeatSortColumn(sortCol.name)
		}
		sortMap[sortCol.name] = idx
	}
	scs := sortColumns{
		columns: sortCols,
		colMap:  sortMap,
	}
	return scs, nil
}

func (m *Merger) Merge(ctx context.Context, results []rows.Rows) (rows.Rows, error) {
	// 检测results是否符合条件
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if len(results) == 0 {
		return nil, errs.ErrMergerEmptyRows
	}
	for i := 0; i < len(results); i++ {
		if err := m.checkColumns(results[i]); err != nil {
			return nil, err
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
	return m.initRows(results)
}

func (m *Merger) initRows(results []rows.Rows) (*Rows, error) {
	rs := &Rows{
		rowsList:     results,
		sortColumns:  m.sortColumns,
		mu:           &sync.RWMutex{},
		columns:      m.cols,
		isPreScanAll: m.preScanAll,
	}
	h := &Heap{
		h:           make([]*node, 0, len(rs.rowsList)),
		sortColumns: rs.sortColumns,
	}
	rs.hp = h
	var err error
	for i := 0; i < len(rs.rowsList); i++ {
		if m.preScanAll {
			err = rs.preScanAll(rs.rowsList[i], i)
		} else {
			err = rs.preScanOne(rs.rowsList[i], i)
		}
		if err != nil {
			_ = rs.Close()
			return nil, err
		}
	}
	return rs, nil
}

func (m *Merger) checkColumns(rows rows.Rows) error {
	if rows == nil {
		return errs.ErrMergerRowsIsNull
	}
	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	colMap := make(map[string]struct{}, len(cols))
	if len(m.cols) == 0 {
		m.cols = cols
	}
	if len(m.cols) != len(cols) {
		return errs.ErrMergerRowsDiff
	}
	for idx, colName := range cols {
		if m.cols[idx] != colName {
			return errs.ErrMergerRowsDiff
		}
		colMap[colName] = struct{}{}
	}

	for _, sortColumn := range m.sortColumns.columns {
		_, ok := colMap[sortColumn.name]
		if !ok {
			return errs.NewInvalidSortColumn(sortColumn.name)
		}
	}
	return nil
}

func newNode(row rows.Rows, sortCols sortColumns, index int) (*node, error) {
	colsInfo, err := row.ColumnTypes()
	fmt.Printf("row err = %#v\n", err)
	if err != nil {
		return nil, err
	}
	columns := make([]any, 0, len(colsInfo))
	sortColumns := make([]any, sortCols.Len())
	for _, colInfo := range colsInfo {
		colName := colInfo.Name()
		colType := colInfo.ScanType()
		for colType.Kind() == reflect.Ptr {
			colType = colType.Elem()
		}
		log.Printf("colName = %s, colType = %s\n", colName, colType.String())
		column := reflect.New(colType).Interface()
		if sortCols.Has(colName) {
			log.Printf("sortCols = %#v, colName = %s, colType = %s\n", sortCols, colName, colType.String())
			sortIndex := sortCols.Find(colName)
			sortColumns[sortIndex] = column
		}
		columns = append(columns, column)
	}
	err = row.Scan(columns...)
	if err != nil {
		return nil, err
	}
	for i := 0; i < len(sortColumns); i++ {
		sortColumns[i] = reflect.ValueOf(sortColumns[i]).Elem().Interface()
	}
	for i := 0; i < len(columns); i++ {
		columns[i] = reflect.ValueOf(columns[i]).Elem().Interface()
	}
	log.Printf("sortColumns = %#v, columns = %#v\n", sortColumns, columns)
	return &node{
		sortCols: sortColumns,
		columns:  columns,
		index:    index,
	}, nil
}

type Rows struct {
	rowsList     []rows.Rows
	sortColumns  sortColumns
	hp           *Heap
	cur          *node
	mu           *sync.RWMutex
	lastErr      error
	closed       bool
	columns      []string
	isPreScanAll bool
}

func (r *Rows) ColumnTypes() ([]*sql.ColumnType, error) {
	return r.rowsList[0].ColumnTypes()
}

func (*Rows) NextResultSet() bool {
	return false
}

func (r *Rows) Next() bool {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return false
	}
	if r.hp.Len() == 0 || r.lastErr != nil {
		r.mu.Unlock()
		_ = r.Close()
		return false
	}
	r.cur = heap.Pop(r.hp).(*node)
	log.Printf("heap node = %#v\n", r.cur)
	if !r.isPreScanAll {
		row := r.rowsList[r.cur.index]
		err := r.preScanOne(row, r.cur.index)
		if err != nil {
			r.lastErr = err
			r.mu.Unlock()
			_ = r.Close()
			return false
		}
	}

	r.mu.Unlock()
	return true
}

func (r *Rows) preScanAll(row rows.Rows, index int) error {
	// TODO Rows抽象之前的假设 rowList中每个sql.Rows中的数据都是已经排序过的
	// 所以只需要读取每个sql.Rows的第一行数据,进行比较就可以得到正确答案
	// 但当使用在pipline中时,就可能需要读取全部sql.Rows中的数据进行排序才能得到正确答案
	// 当然可以进行针对性的优化——两种读模式,一次读一行,一次读全部
	for row.Next() {
		n, err := newNode(row, r.sortColumns, index)
		if err != nil {
			return err
		}
		heap.Push(r.hp, n)
	}
	return row.Err()
}

func (r *Rows) preScanOne(row rows.Rows, index int) error {
	if row.Next() {
		n, err := newNode(row, r.sortColumns, index)
		if err != nil {
			return err
		}
		heap.Push(r.hp, n)
	} else if row.Err() != nil {
		return row.Err()
	}
	return nil
}

func (r *Rows) Scan(dest ...any) error {
	log.Printf("Scan .......")
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.lastErr != nil {
		return r.lastErr
	}
	if r.closed {
		return errs.ErrMergerRowsClosed
	}
	if r.cur == nil {
		return errs.ErrMergerScanNotNext
	}
	var err error
	for i := 0; i < len(dest); i++ {
		err = rows.ConvertAssign(dest[i], r.cur.columns[i])
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *Rows) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	errorList := make([]error, 0, len(r.rowsList))
	for i := 0; i < len(r.rowsList); i++ {
		row := r.rowsList[i]
		err := row.Close()
		if err != nil {
			errorList = append(errorList, err)
		}
	}
	return multierr.Combine(errorList...)
}

func (r *Rows) Err() error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.lastErr
}

func (r *Rows) Columns() ([]string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return nil, errs.ErrMergerRowsClosed
	}
	return r.columns, nil
}
