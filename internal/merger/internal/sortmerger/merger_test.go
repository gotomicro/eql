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
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"

	_ "github.com/mattn/go-sqlite3"

	"github.com/ecodeclub/eorm/internal/rows"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/ecodeclub/eorm/internal/merger/internal/errs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/multierr"
)

var (
	nextMockErr error = errors.New("rows: MockNextErr")
)

func newCloseMockErr(dbName string) error {
	return fmt.Errorf("rows: %s MockCloseErr", dbName)
}

type MergerSuite struct {
	suite.Suite
	mockDB01 *sql.DB
	mock01   sqlmock.Sqlmock
	mockDB02 *sql.DB
	mock02   sqlmock.Sqlmock
	mockDB03 *sql.DB
	mock03   sqlmock.Sqlmock
	mockDB04 *sql.DB
	mock04   sqlmock.Sqlmock
}

func (ms *MergerSuite) SetupTest() {
	t := ms.T()
	ms.initMock(t)
}

func (ms *MergerSuite) TearDownTest() {
	_ = ms.mockDB01.Close()
	_ = ms.mockDB02.Close()
	_ = ms.mockDB03.Close()
	_ = ms.mockDB04.Close()
}

func (ms *MergerSuite) initMock(t *testing.T) {
	var err error
	ms.mockDB01, ms.mock01, err = sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	ms.mockDB02, ms.mock02, err = sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	ms.mockDB03, ms.mock03, err = sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	ms.mockDB04, ms.mock04, err = sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
}

func (ms *MergerSuite) TestMerger_New() {
	testcases := []struct {
		name           string
		wantErr        error
		wantSortColumn func() []SortColumn
		sortCols       []SortColumn
	}{
		{
			name: "正常案例",
			wantSortColumn: func() []SortColumn {
				sortCol := NewSortColumn("id", ASC)
				return []SortColumn{sortCol}
			},
			sortCols: []SortColumn{
				NewSortColumn("id", ASC),
			},
		},
		{
			name:     "空的排序列表",
			sortCols: []SortColumn{},
			wantErr:  errs.ErrEmptySortColumns,
		},
		{
			name: "排序列重复",
			sortCols: []SortColumn{
				NewSortColumn("id", ASC),
				NewSortColumn("id", DESC),
			},
			wantErr: errs.NewRepeatSortColumn("id"),
		},
	}
	for _, tc := range testcases {
		ms.T().Run(tc.name, func(t *testing.T) {
			mer, err := NewMerger(false, tc.sortCols...)
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantSortColumn(), mer.sortColumns.columns)
		})
	}
}

func (ms *MergerSuite) TestMerger_Merge() {
	testcases := []struct {
		name    string
		merger  func() (*Merger, error)
		ctx     func() (context.Context, context.CancelFunc)
		wantErr error
		sqlRows func() []rows.Rows
		after   func(t *testing.T, r rows.Rows)
	}{
		{
			name: "sqlRows字段不同",
			merger: func() (*Merger, error) {
				return NewMerger(false, NewSortColumn("id", ASC))
			},
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			sqlRows: func() []rows.Rows {
				query := "SELECT * FROM `t1`"
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows([]string{"id", "name", "address"}).AddRow(1, "abex", "cn").AddRow(5, "bruce", "cn"))
				ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows([]string{"id", "name", "email"}).AddRow(3, "alex", "cn").AddRow(4, "x", "cn"))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02}
				rowsList := make([]rows.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			wantErr: errs.ErrMergerRowsDiff,
		},
		{
			name: "sqlRows字段不同_少一个字段",
			merger: func() (*Merger, error) {
				return NewMerger(false, NewSortColumn("id", ASC))
			},
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			sqlRows: func() []rows.Rows {
				query := "SELECT * FROM `t1`"
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows([]string{"id", "name", "address"}).AddRow(1, "abex", "cn").AddRow(5, "bruce", "cn"))
				ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(3, "alex").AddRow(4, "x"))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02}
				rowsList := make([]rows.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			wantErr: errs.ErrMergerRowsDiff,
		},
		{
			name: "超时",
			merger: func() (*Merger, error) {
				return NewMerger(false, NewSortColumn("id", ASC))
			},
			ctx: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithTimeout(context.Background(), 0)
				return ctx, cancel
			},
			wantErr: context.DeadlineExceeded,
			sqlRows: func() []rows.Rows {
				query := "SELECT * FROM `t1`;"
				cols := []string{"id"}
				res := make([]rows.Rows, 0, 1)
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(1))
				rows, _ := ms.mockDB01.QueryContext(context.Background(), query)
				res = append(res, rows)
				return res
			},
		},
		{
			name: "sqlRows列表为空",
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			merger: func() (*Merger, error) {
				return NewMerger(false, NewSortColumn("id", ASC))
			},
			sqlRows: func() []rows.Rows {
				return []rows.Rows{}
			},
			wantErr: errs.ErrMergerEmptyRows,
		},
		{
			name: "sqlRows列表有nil",
			merger: func() (*Merger, error) {
				return NewMerger(false, NewSortColumn("id", ASC))
			},
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			sqlRows: func() []rows.Rows {
				return []rows.Rows{nil}
			},
			wantErr: errs.ErrMergerRowsIsNull,
		},
		{
			name: "数据库列集: id;排序列集: age",
			merger: func() (*Merger, error) {
				return NewMerger(false, NewSortColumn("age", ASC))
			},
			sqlRows: func() []rows.Rows {
				query := "SELECT * FROM `t1`;"
				cols := []string{"id"}
				res := make([]rows.Rows, 0, 1)
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(1))
				rows, _ := ms.mockDB01.QueryContext(context.Background(), query)
				res = append(res, rows)
				return res
			},
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			wantErr: errs.NewInvalidSortColumn("age"),
		},
		{
			name: "数据库列集: id;排序列集: id,age",
			merger: func() (*Merger, error) {
				return NewMerger(false, NewSortColumn("id", ASC), NewSortColumn("age", ASC))
			},
			sqlRows: func() []rows.Rows {
				query := "SELECT * FROM `t1`;"
				cols := []string{"id"}
				res := make([]rows.Rows, 0, 1)
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(1))
				rows, _ := ms.mockDB01.QueryContext(context.Background(), query)
				res = append(res, rows)
				return res
			},
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			wantErr: errs.NewInvalidSortColumn("age"),
		},
		{
			name: "数据库列集: id,name,address;排序列集: age",
			merger: func() (*Merger, error) {
				return NewMerger(false, NewSortColumn("age", ASC))
			},
			sqlRows: func() []rows.Rows {
				query := "SELECT * FROM `t1`;"
				cols := []string{"id", "name", "address"}
				res := make([]rows.Rows, 0, 1)
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "zwl", "sh"))
				rows, _ := ms.mockDB01.QueryContext(context.Background(), query)
				res = append(res, rows)
				return res
			},
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			wantErr: errs.NewInvalidSortColumn("age"),
		},
		{
			name: "数据库列集: id,name,address;排序列集: id,age,name",
			merger: func() (*Merger, error) {
				return NewMerger(false, NewSortColumn("id", ASC), NewSortColumn("age", ASC), NewSortColumn("name", ASC))
			},
			sqlRows: func() []rows.Rows {
				query := "SELECT * FROM `t1`;"
				cols := []string{"id", "name", "address"}
				res := make([]rows.Rows, 0, 1)
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "zwl", "sh"))
				rows, _ := ms.mockDB01.QueryContext(context.Background(), query)
				res = append(res, rows)
				return res
			},
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			wantErr: errs.NewInvalidSortColumn("age"),
		},
		{
			name: "数据库列集: id,name,address;排序列集: id,name,age",
			merger: func() (*Merger, error) {
				return NewMerger(false, NewSortColumn("id", ASC), NewSortColumn("name", ASC), NewSortColumn("age", ASC))
			},
			sqlRows: func() []rows.Rows {
				query := "SELECT * FROM `t1`;"
				cols := []string{"id", "name", "address"}
				res := make([]rows.Rows, 0, 1)
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "zwl", "sh"))
				rows, _ := ms.mockDB01.QueryContext(context.Background(), query)
				res = append(res, rows)
				return res
			},
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			wantErr: errs.NewInvalidSortColumn("age"),
		},
		{
			name: "数据库列集: id ;排序列集: id",
			merger: func() (*Merger, error) {
				return NewMerger(false, NewSortColumn("id", ASC))
			},
			sqlRows: func() []rows.Rows {
				query := "SELECT * FROM `t1`;"
				cols := []string{"id"}
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(1).AddRow(5))
				ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(2).AddRow(3))
				ms.mock03.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(4).AddRow(6))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
				rowsList := make([]rows.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			after: func(t *testing.T, r rows.Rows) {
				t.Helper()

				cols := []string{"id"}
				columnsNames, err := r.Columns()
				require.NoError(t, err)
				require.Equal(t, cols, columnsNames)

				scanFunc := func(rr rows.Rows, valSet *[]any) error {
					var uid int
					if err := rr.Scan(&uid); err != nil {
						return err
					}
					*valSet = append(*valSet, []any{uid})
					return nil
				}

				require.Equal(t, []any{
					[]any{1},
					[]any{2},
					[]any{3},
					[]any{4},
					[]any{5},
					[]any{6},
				}, ms.getRowValues(t, r, scanFunc))
			},
		},
		{
			name: "数据库列集: id,age;排序列集: id,age",
			merger: func() (*Merger, error) {
				return NewMerger(false, NewSortColumn("id", ASC), NewSortColumn("age", ASC))
			},
			sqlRows: func() []rows.Rows {
				query := "SELECT * FROM `t1`;"
				cols := []string{"id", "age"}
				res := make([]rows.Rows, 0, 1)
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(1, 18))
				rows, _ := ms.mockDB01.QueryContext(context.Background(), query)
				res = append(res, rows)
				return res
			},
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			after: func(t *testing.T, r rows.Rows) {
				t.Helper()
			},
		},
		{
			name: "数据库列集: id,name,address;排序列集: id,name",
			merger: func() (*Merger, error) {
				return NewMerger(true, NewSortColumn("name", ASC), NewSortColumn("address", DESC))
			},
			sqlRows: func() []rows.Rows {
				query := "SELECT * FROM `t1`;"
				cols := []string{"id", "name", "address"}
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(5, "curry", "cn").AddRow(1, "zwl", "sh"))
				ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(2, "alex", "cn").AddRow(3, "curry", "jp"))
				ms.mock03.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(4, "bob", "tw").AddRow(6, "david", "hk"))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
				rowsList := make([]rows.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			after: func(t *testing.T, r rows.Rows) {
				t.Helper()

				cols := []string{"id", "name", "address"}
				columnsNames, err := r.Columns()
				require.NoError(t, err)
				require.Equal(t, cols, columnsNames)

				scanFunc := func(rr rows.Rows, valSet *[]any) error {
					var uid int
					var name, address string
					if err := rr.Scan(&uid, &name, &address); err != nil {
						return err
					}
					*valSet = append(*valSet, []any{uid, name, address})
					return nil
				}

				require.Equal(t, []any{
					[]any{2, "alex", "cn"},
					[]any{4, "bob", "tw"},
					[]any{3, "curry", "jp"},
					[]any{5, "curry", "cn"},
					[]any{6, "david", "hk"},
					[]any{1, "zwl", "sh"},
				}, ms.getRowValues(t, r, scanFunc))
			},
		},
		{
			name: "初始化Rows错误",
			merger: func() (*Merger, error) {
				return NewMerger(false, NewSortColumn("id", ASC))
			},
			sqlRows: func() []rows.Rows {
				query := "SELECT * FROM `t1`;"
				cols := []string{"id", "name", "address"}
				res := make([]rows.Rows, 0, 1)
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "zwl", "sh").RowError(0, nextMockErr))
				rows, _ := ms.mockDB01.QueryContext(context.Background(), query)
				res = append(res, rows)
				return res
			},
			wantErr: nextMockErr,
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
		},
	}
	for _, tc := range testcases {
		ms.T().Run(tc.name, func(t *testing.T) {
			merger, err := tc.merger()
			require.NoError(ms.T(), err)
			ctx, cancel := tc.ctx()
			rows, err := merger.Merge(ctx, tc.sqlRows())
			cancel()
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			require.NotNil(t, rows)
			tc.after(t, rows)
		})

	}
}

func (ms *MergerSuite) getRowValues(t *testing.T, r rows.Rows, scanFunc func(r rows.Rows, valSet *[]any) error) []any {
	var res []any
	for r.Next() {
		require.NoError(t, scanFunc(r, &res))
	}
	return res
}

func (ms *MergerSuite) TestRows_NextAndScan() {
	testCases := []struct {
		name        string
		sqlRows     func() []rows.Rows
		wantVal     []TestModel
		sortColumns []SortColumn
		wantErr     error
	}{
		{
			name: "完全交叉读，sqlRows返回行数相同",
			sqlRows: func() []rows.Rows {
				cols := []string{"id", "name", "address"}
				query := "SELECT * FROM `t1`"
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "abex", "cn").AddRow(5, "bruce", "cn"))
				ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(3, "alex", "cn").AddRow(4, "x", "cn"))
				ms.mock03.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(2, "a", "cn").AddRow(7, "b", "cn"))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
				rowsList := make([]rows.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			wantVal: []TestModel{
				{
					Id:      1,
					Name:    "abex",
					Address: "cn",
				},
				{
					Id:      2,
					Name:    "a",
					Address: "cn",
				},
				{
					Id:      3,
					Name:    "alex",
					Address: "cn",
				},
				{
					Id:      4,
					Name:    "x",
					Address: "cn",
				},
				{
					Id:      5,
					Name:    "bruce",
					Address: "cn",
				},
				{
					Id:      7,
					Name:    "b",
					Address: "cn",
				},
			},
			sortColumns: []SortColumn{
				NewSortColumn("id", ASC),
			},
		},
		{
			name: "完全交叉读，sqlRows返回行数部分不同",
			sqlRows: func() []rows.Rows {
				cols := []string{"id", "name", "address"}
				query := "SELECT * FROM `t1`"
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(7, "b", "cn").AddRow(6, "x", "cn").AddRow(1, "x", "cn"))
				ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(8, "alex", "cn").AddRow(4, "bruce", "cn"))
				ms.mock03.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(9, "a", "cn").AddRow(5, "abex", "cn"))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
				rowsList := make([]rows.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			wantVal: []TestModel{
				{
					Id:      9,
					Name:    "a",
					Address: "cn",
				},
				{
					Id:      8,
					Name:    "alex",
					Address: "cn",
				},
				{
					Id:      7,
					Name:    "b",
					Address: "cn",
				},
				{
					Id:      6,
					Name:    "x",
					Address: "cn",
				},
				{
					Id:      5,
					Name:    "abex",
					Address: "cn",
				},
				{
					Id:      4,
					Name:    "bruce",
					Address: "cn",
				},
				{
					Id:      1,
					Name:    "x",
					Address: "cn",
				},
			},
			sortColumns: []SortColumn{
				NewSortColumn("id", DESC),
			},
		},
		{
			// 包含一个sqlRows返回的行数为0，在前面
			name: "完全交叉读，sqlRows返回行数完全不同",
			sqlRows: func() []rows.Rows {
				cols := []string{"id", "name", "address"}
				query := "SELECT * FROM `t1`"
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "c", "cn").AddRow(2, "bruce", "cn").AddRow(2, "zwl", "cn"))
				ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "alex", "cn").AddRow(3, "x", "cn"))
				ms.mock03.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(2, "c", "cn").AddRow(3, "b", "cn").AddRow(5, "c", "cn").AddRow(7, "c", "cn"))
				ms.mock04.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols))
				dbs := []*sql.DB{ms.mockDB04, ms.mockDB01, ms.mockDB02, ms.mockDB03}
				rowsList := make([]rows.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			wantVal: []TestModel{
				{
					Id:      1,
					Name:    "alex",
					Address: "cn",
				},
				{
					Id:      1,
					Name:    "c",
					Address: "cn",
				},
				{
					Id:      2,
					Name:    "bruce",
					Address: "cn",
				},
				{
					Id:      2,
					Name:    "c",
					Address: "cn",
				},
				{
					Id:      2,
					Name:    "zwl",
					Address: "cn",
				},
				{
					Id:      3,
					Name:    "b",
					Address: "cn",
				},
				{
					Id:      3,
					Name:    "x",
					Address: "cn",
				},
				{
					Id:      5,
					Name:    "c",
					Address: "cn",
				},
				{
					Id:      7,
					Name:    "c",
					Address: "cn",
				},
			},
			sortColumns: []SortColumn{
				NewSortColumn("id", ASC),
				NewSortColumn("name", ASC),
			},
		},
		{
			name: "部分交叉读，sqlRows返回行数相同",
			sqlRows: func() []rows.Rows {
				cols := []string{"id", "name", "address"}
				query := "SELECT * FROM `t1`"
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "abex", "cn").AddRow(2, "a", "cn"))
				ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(3, "alex", "cn").AddRow(5, "bruce", "cn"))
				ms.mock03.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(4, "x", "cn").AddRow(7, "b", "cn"))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
				rowsList := make([]rows.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			wantVal: []TestModel{
				{
					Id:      1,
					Name:    "abex",
					Address: "cn",
				},
				{
					Id:      2,
					Name:    "a",
					Address: "cn",
				},
				{
					Id:      3,
					Name:    "alex",
					Address: "cn",
				},
				{
					Id:      4,
					Name:    "x",
					Address: "cn",
				},
				{
					Id:      5,
					Name:    "bruce",
					Address: "cn",
				},
				{
					Id:      7,
					Name:    "b",
					Address: "cn",
				},
			},
			sortColumns: []SortColumn{
				NewSortColumn("id", ASC),
			},
		},
		{
			name: "部分交叉读，sqlRows返回行数部分相同",
			sqlRows: func() []rows.Rows {
				cols := []string{"id", "name", "address"}
				query := "SELECT * FROM `t1`"
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "abex", "cn").AddRow(2, "a", "cn").AddRow(5, "bruce", "cn"))
				ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(3, "alex", "cn").AddRow(4, "x", "cn"))
				ms.mock03.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(7, "b", "cn").AddRow(8, "b", "cn"))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
				rowsList := make([]rows.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			wantVal: []TestModel{
				{
					Id:      1,
					Name:    "abex",
					Address: "cn",
				},
				{
					Id:      2,
					Name:    "a",
					Address: "cn",
				},
				{
					Id:      3,
					Name:    "alex",
					Address: "cn",
				},
				{
					Id:      4,
					Name:    "x",
					Address: "cn",
				},
				{
					Id:      5,
					Name:    "bruce",
					Address: "cn",
				},
				{
					Id:      7,
					Name:    "b",
					Address: "cn",
				},
				{
					Id:      8,
					Name:    "b",
					Address: "cn",
				},
			},
			sortColumns: []SortColumn{
				NewSortColumn("id", ASC),
			},
		},
		{
			// 包含一个sqlRows返回的行数为0，在中间
			name: "部分交叉读，sqlRows返回行数完全不同",
			sqlRows: func() []rows.Rows {
				cols := []string{"id", "name", "address"}
				query := "SELECT * FROM `t1`"
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "abex", "cn").AddRow(2, "a", "cn").AddRow(5, "bruce", "cn"))
				ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(3, "alex", "cn").AddRow(4, "x", "cn"))
				ms.mock03.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(7, "b", "cn"))
				ms.mock04.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB04, ms.mockDB03}
				rowsList := make([]rows.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			wantVal: []TestModel{
				{
					Id:      1,
					Name:    "abex",
					Address: "cn",
				},
				{
					Id:      2,
					Name:    "a",
					Address: "cn",
				},
				{
					Id:      3,
					Name:    "alex",
					Address: "cn",
				},
				{
					Id:      4,
					Name:    "x",
					Address: "cn",
				},
				{
					Id:      5,
					Name:    "bruce",
					Address: "cn",
				},
				{
					Id:      7,
					Name:    "b",
					Address: "cn",
				},
			},
			sortColumns: []SortColumn{
				NewSortColumn("id", ASC),
			},
		},
		{
			name: "顺序读，sqlRows返回行数相同",
			sqlRows: func() []rows.Rows {
				cols := []string{"id", "name", "address"}
				query := "SELECT * FROM `t1`"
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "abex", "cn").AddRow(2, "a", "cn"))
				ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(3, "alex", "cn").AddRow(4, "x", "cn"))
				ms.mock03.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(5, "bruce", "cn").AddRow(7, "b", "cn"))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
				rowsList := make([]rows.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			wantVal: []TestModel{
				{
					Id:      1,
					Name:    "abex",
					Address: "cn",
				},
				{
					Id:      2,
					Name:    "a",
					Address: "cn",
				},
				{
					Id:      3,
					Name:    "alex",
					Address: "cn",
				},
				{
					Id:      4,
					Name:    "x",
					Address: "cn",
				},
				{
					Id:      5,
					Name:    "bruce",
					Address: "cn",
				},
				{
					Id:      7,
					Name:    "b",
					Address: "cn",
				},
			},
			sortColumns: []SortColumn{
				NewSortColumn("id", ASC),
			},
		},
		{
			name: "顺序读，sqlRows返回行数部分不同",
			sqlRows: func() []rows.Rows {
				cols := []string{"id", "name", "address"}
				query := "SELECT * FROM `t1`"
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "abex", "cn").AddRow(2, "a", "cn"))
				ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(3, "alex", "cn").AddRow(4, "x", "cn"))
				ms.mock03.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(5, "bruce", "cn"))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
				rowsList := make([]rows.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},

			wantVal: []TestModel{
				{
					Id:      1,
					Name:    "abex",
					Address: "cn",
				},
				{
					Id:      2,
					Name:    "a",
					Address: "cn",
				},
				{
					Id:      3,
					Name:    "alex",
					Address: "cn",
				},
				{
					Id:      4,
					Name:    "x",
					Address: "cn",
				},
				{
					Id:      5,
					Name:    "bruce",
					Address: "cn",
				},
			},
			sortColumns: []SortColumn{
				NewSortColumn("id", ASC),
			},
		},
		{
			// 包含一个sqlRows返回的行数为0，在后面
			name: "顺序读，sqlRows返回行数完全不同",
			sqlRows: func() []rows.Rows {
				cols := []string{"id", "name", "address"}
				query := "SELECT * FROM `t1`"
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "abex", "cn"))
				ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(2, "a", "cn").AddRow(3, "alex", "cn"))
				ms.mock03.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(4, "x", "cn").AddRow(5, "bruce", "cn").AddRow(7, "b", "cn"))
				ms.mock04.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03, ms.mockDB04}
				rowsList := make([]rows.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			wantVal: []TestModel{
				{
					Id:      1,
					Name:    "abex",
					Address: "cn",
				},
				{
					Id:      2,
					Name:    "a",
					Address: "cn",
				},
				{
					Id:      3,
					Name:    "alex",
					Address: "cn",
				},
				{
					Id:      4,
					Name:    "x",
					Address: "cn",
				},
				{
					Id:      5,
					Name:    "bruce",
					Address: "cn",
				},
				{
					Id:      7,
					Name:    "b",
					Address: "cn",
				},
			},
			sortColumns: []SortColumn{
				NewSortColumn("id", ASC),
			},
		},

		{
			name: "所有sqlRows返回的行数均为空",
			sqlRows: func() []rows.Rows {
				cols := []string{"id", "name", "address"}
				query := "SELECT * FROM `t1`"
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols))
				ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols))
				ms.mock03.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols))
				ms.mock04.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03, ms.mockDB04}
				rowsList := make([]rows.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			wantVal: []TestModel{},
			sortColumns: []SortColumn{
				NewSortColumn("id", ASC),
				NewSortColumn("name", ASC),
			},
		},
		{
			name: "排序列返回的顺序和数据库里的字段顺序不一致",
			sqlRows: func() []rows.Rows {
				cols := []string{"id", "name", "address"}
				query := "SELECT * FROM `t1`"
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(2, "a", "hz").AddRow(3, "b", "hz").AddRow(2, "b", "cs"))
				ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(3, "a", "cs").AddRow(1, "a", "cs").AddRow(3, "e", "cn"))
				ms.mock03.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(2, "d", "hm").AddRow(5, "k", "xx").AddRow(4, "k", "xz"))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
				rowsList := make([]rows.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			wantVal: []TestModel{
				{
					Id:      3,
					Name:    "a",
					Address: "cs",
				},
				{
					Id:      2,
					Name:    "a",
					Address: "hz",
				},
				{
					Id:      1,
					Name:    "a",
					Address: "cs",
				},
				{
					Id:      3,
					Name:    "b",
					Address: "hz",
				},
				{
					Id:      2,
					Name:    "b",
					Address: "cs",
				},
				{
					Id:      2,
					Name:    "d",
					Address: "hm",
				},
				{
					Id:      3,
					Name:    "e",
					Address: "cn",
				},
				{
					Id:      5,
					Name:    "k",
					Address: "xx",
				},
				{
					Id:      4,
					Name:    "k",
					Address: "xz",
				},
			},
			sortColumns: []SortColumn{
				NewSortColumn("name", ASC),
				NewSortColumn("id", DESC),
			},
		},
	}
	for _, tc := range testCases {
		ms.T().Run(tc.name, func(t *testing.T) {
			merger, err := NewMerger(false, tc.sortColumns...)
			require.NoError(t, err)
			rows, err := merger.Merge(context.Background(), tc.sqlRows())
			require.NoError(t, err)
			res := make([]TestModel, 0, len(tc.wantVal))
			for rows.Next() {
				t := TestModel{}
				err := rows.Scan(&t.Id, &t.Name, &t.Address)
				require.NoError(ms.T(), err)
				res = append(res, t)
			}
			require.True(t, rows.(*Rows).closed)
			assert.NoError(t, rows.Err())
			assert.Equal(t, tc.wantVal, res)
		})
	}

}

func (ms *MergerSuite) TestRows_Columns() {
	cols := []string{"id"}
	query := "SELECT * FROM `t1`"
	ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow("1"))
	ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow("2"))
	ms.mock03.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow("3").AddRow("4"))
	merger, err := NewMerger(false, NewSortColumn("id", DESC))
	require.NoError(ms.T(), err)
	dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
	rowsList := make([]rows.Rows, 0, len(dbs))
	for _, db := range dbs {
		row, err := db.QueryContext(context.Background(), query)
		require.NoError(ms.T(), err)
		rowsList = append(rowsList, row)
	}

	rows, err := merger.Merge(context.Background(), rowsList)
	require.NoError(ms.T(), err)
	ms.T().Run("Next没有迭代完", func(t *testing.T) {
		for rows.Next() {
			columns, err := rows.Columns()
			require.NoError(t, err)
			assert.Equal(t, cols, columns)
		}
		require.NoError(t, rows.Err())
	})
	ms.T().Run("Next迭代完", func(t *testing.T) {
		require.False(t, rows.Next())
		require.NoError(t, rows.Err())
		_, err := rows.Columns()
		assert.Equal(t, errs.ErrMergerRowsClosed, err)

	})

}

func (ms *MergerSuite) TestRows_Close() {
	cols := []string{"id"}
	query := "SELECT * FROM `t1`"
	ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow("1"))
	ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow("2").CloseError(newCloseMockErr("db02")))
	ms.mock03.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow("3").AddRow("4").CloseError(newCloseMockErr("db03")))
	merger, err := NewMerger(false, NewSortColumn("id", DESC))
	require.NoError(ms.T(), err)
	dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
	rowsList := make([]rows.Rows, 0, len(dbs))
	for _, db := range dbs {
		row, err := db.QueryContext(context.Background(), query)
		require.NoError(ms.T(), err)
		rowsList = append(rowsList, row)
	}
	rows, err := merger.Merge(context.Background(), rowsList)
	require.NoError(ms.T(), err)
	// 判断当前是可以正常读取的
	require.True(ms.T(), rows.Next())
	var id int
	err = rows.Scan(&id)
	require.NoError(ms.T(), err)
	err = rows.Close()
	ms.T().Run("close返回multierror", func(t *testing.T) {
		assert.Equal(ms.T(), multierr.Combine(newCloseMockErr("db02"), newCloseMockErr("db03")), err)
	})
	ms.T().Run("close之后Next返回false", func(t *testing.T) {
		for i := 0; i < len(rowsList); i++ {
			require.False(ms.T(), rowsList[i].Next())
		}
		require.False(ms.T(), rows.Next())
	})
	ms.T().Run("close之后Scan返回迭代过程中的错误", func(t *testing.T) {
		var id int
		err := rows.Scan(&id)
		assert.Equal(t, errs.ErrMergerRowsClosed, err)
	})
	ms.T().Run("close之后调用Columns方法返回错误", func(t *testing.T) {
		_, err := rows.Columns()
		require.Error(t, err)
	})
	ms.T().Run("close多次是等效的", func(t *testing.T) {
		for i := 0; i < 4; i++ {
			err = rows.Close()
			require.NoError(t, err)
		}
	})
}

// 测试Next迭代过程中遇到错误
func (ms *MergerSuite) TestRows_NextAndErr() {
	testcases := []struct {
		name        string
		rowsList    func() []rows.Rows
		wantErr     error
		sortColumns []SortColumn
	}{
		{
			name: "sqlRows列表中有一个返回error",
			rowsList: func() []rows.Rows {
				cols := []string{"id"}
				query := "SELECT * FROM `t1`"
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow("1"))
				ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow("2"))
				ms.mock03.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow("3").AddRow("4").RowError(1, nextMockErr))
				ms.mock04.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow("5"))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03, ms.mockDB04}
				rowsList := make([]rows.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			sortColumns: []SortColumn{
				NewSortColumn("id", ASC),
			},
			wantErr: nextMockErr,
		},
	}
	for _, tc := range testcases {
		ms.T().Run(tc.name, func(t *testing.T) {
			merger, err := NewMerger(false, tc.sortColumns...)
			require.NoError(t, err)
			rows, err := merger.Merge(context.Background(), tc.rowsList())
			require.NoError(t, err)
			for rows.Next() {
			}
			assert.Equal(t, tc.wantErr, rows.Err())
		})
	}
}

// Scan方法的一些边界情况的测试
func (ms *MergerSuite) TestRows_ScanErr() {
	ms.T().Run("未调用Next，直接Scan，返回错", func(t *testing.T) {
		cols := []string{"id", "name", "address"}
		query := "SELECT * FROM `t1`"
		ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "abex", "cn").AddRow(5, "bruce", "cn"))
		r, err := ms.mockDB01.QueryContext(context.Background(), query)
		require.NoError(t, err)
		rowsList := []rows.Rows{r}
		merger, err := NewMerger(false, NewSortColumn("id", DESC))
		require.NoError(t, err)
		rows, err := merger.Merge(context.Background(), rowsList)
		require.NoError(t, err)
		model := TestModel{}
		err = rows.Scan(&model.Id, &model.Name, &model.Address)
		assert.Equal(t, errs.ErrMergerScanNotNext, err)
	})
	ms.T().Run("迭代过程中发现错误,调用Scan，返回迭代中发现的错误", func(t *testing.T) {
		cols := []string{"id", "name", "address"}
		query := "SELECT * FROM `t1`"
		ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "abex", "cn").AddRow(5, "bruce", "cn").RowError(1, nextMockErr))
		r, err := ms.mockDB01.QueryContext(context.Background(), query)
		require.NoError(t, err)
		rowsList := []rows.Rows{r}
		merger, err := NewMerger(false, NewSortColumn("id", DESC))
		require.NoError(t, err)
		rows, err := merger.Merge(context.Background(), rowsList)
		require.NoError(t, err)
		for rows.Next() {
		}
		var model TestModel
		err = rows.Scan(&model.Id, &model.Name, &model.Address)
		assert.Equal(t, nextMockErr, err)
	})

}

func (ms *MergerSuite) TestRows_ColumnTypes() {
	t := ms.T()

	query := "SELECT `grade` FROM `t1` ORDER BY `grade` DESC"
	cols := []string{"`grade`"}
	ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(100))
	ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(90))
	ms.mock03.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(110))

	sortColumns := []SortColumn{
		{
			name:  "`grade`",
			order: DESC,
		},
	}
	m, err := NewMerger(true, sortColumns...)
	require.NoError(t, err)

	r, err := m.Merge(context.Background(), getResultSet(t, query, ms.mockDB01, ms.mockDB02, ms.mockDB03))
	require.NoError(t, err)

	t.Run("rows未关闭", func(t *testing.T) {
		types, err := r.ColumnTypes()
		require.NoError(t, err)

		names := make([]string, 0, len(types))
		for _, typ := range types {
			names = append(names, typ.Name())
		}
		require.Equal(t, []string{"`grade`"}, names)
	})

	t.Run("rows已关闭", func(t *testing.T) {
		require.NoError(t, r.Close())

		_, err = r.ColumnTypes()
		require.ErrorIs(t, err, errs.ErrMergerRowsClosed)
	})
}

func getResultSet(t *testing.T, sql string, dbs ...*sql.DB) []rows.Rows {
	resultSet := make([]rows.Rows, 0, len(dbs))
	for _, db := range dbs {
		row, err := db.Query(sql)
		require.NoError(t, err)
		resultSet = append(resultSet, row)
	}
	return resultSet
}

type TestModel struct {
	Id      int
	Name    string
	Address string
}

func TestMerger(t *testing.T) {
	suite.Run(t, &MergerSuite{})
	suite.Run(t, &NullableMergerSuite{})
}

type NullableMergerSuite struct {
	suite.Suite
	db01 *sql.DB
	db02 *sql.DB
	db03 *sql.DB
}

func (ms *NullableMergerSuite) SetupSuite() {
	t := ms.T()
	query := "CREATE TABLE t1 (\n      id int primary key,\n      `age`  int,\n    \t`name` varchar(20)\n  );\n"
	db01, err := sql.Open("sqlite3", "file:test01.db?cache=shared&mode=memory")
	if err != nil {
		t.Fatal(err)
	}
	ms.db01 = db01
	_, err = db01.ExecContext(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	db02, err := sql.Open("sqlite3", "file:test02.db?cache=shared&mode=memory")
	if err != nil {
		t.Fatal(err)
	}
	ms.db02 = db02
	_, err = db02.ExecContext(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	db03, err := sql.Open("sqlite3", "file:test03.db?cache=shared&mode=memory")
	if err != nil {
		t.Fatal(err)
	}
	ms.db03 = db03
	_, err = db03.ExecContext(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
}

func (ms *NullableMergerSuite) TearDownSuite() {
	_ = ms.db01.Close()
	_ = ms.db02.Close()
	_ = ms.db03.Close()
}

func (ms *NullableMergerSuite) TestRows_Nullable() {
	testcases := []struct {
		name        string
		rowsList    func() []rows.Rows
		sortColumns []SortColumn
		wantErr     error
		afterFunc   func()
		wantVal     []Nullable
	}{
		{
			name: "多个nullable类型排序 age asc,name desc",
			rowsList: func() []rows.Rows {
				db1InsertSql := []string{
					"insert into t1  (id,  name) values (1,  'zwl')",
					"insert into t1  (id, age, name) values (2, 10, 'zwl')",
					"insert into t1  (id, age, name) values (3, 20, 'zwl')",
					"insert into t1  (id, age) values (4, 20)",
				}
				for _, sql := range db1InsertSql {
					_, err := ms.db01.ExecContext(context.Background(), sql)
					require.NoError(ms.T(), err)
				}
				db2InsertSql := []string{
					"insert into t1  (id, age, name) values (5, 5, 'zwl')",
					"insert into t1  (id, age, name) values (6, 20, 'dm')",
				}
				for _, sql := range db2InsertSql {
					_, err := ms.db02.ExecContext(context.Background(), sql)
					require.NoError(ms.T(), err)
				}
				db3InsertSql := []string{
					"insert into t1  (id, name) values (7, 'xq')",
					"insert into t1  (id, age) values (8, 5)",
					"insert into t1  (id, age,name) values (9, 10,'xx')",
				}
				for _, sql := range db3InsertSql {
					_, err := ms.db03.ExecContext(context.Background(), sql)
					require.NoError(ms.T(), err)
				}
				dbs := []*sql.DB{ms.db01, ms.db02, ms.db03}
				rowsList := make([]rows.Rows, 0, len(dbs))
				query := "SELECT `id`, `age`,`name` FROM `t1` order by age asc,name desc"
				for _, db := range dbs {
					rows, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, rows)
				}
				return rowsList
			},
			sortColumns: []SortColumn{
				NewSortColumn("age", ASC),
				NewSortColumn("name", DESC),
			},
			afterFunc: func() {
				dbs := []*sql.DB{ms.db01, ms.db02, ms.db03}
				for _, db := range dbs {
					_, err := db.Exec("DELETE FROM t1;")
					require.NoError(ms.T(), err)
				}
			},
			wantVal: func() []Nullable {
				return []Nullable{
					{
						Id:   sql.NullInt64{Valid: true, Int64: 1},
						Age:  sql.NullInt64{Valid: false, Int64: 0},
						Name: sql.NullString{Valid: true, String: "zwl"},
					},
					{
						Id:   sql.NullInt64{Valid: true, Int64: 7},
						Age:  sql.NullInt64{Valid: false, Int64: 0},
						Name: sql.NullString{Valid: true, String: "xq"},
					},
					{
						Id:   sql.NullInt64{Valid: true, Int64: 5},
						Age:  sql.NullInt64{Valid: true, Int64: 5},
						Name: sql.NullString{Valid: true, String: "zwl"},
					},
					{
						Id:   sql.NullInt64{Valid: true, Int64: 8},
						Age:  sql.NullInt64{Valid: true, Int64: 5},
						Name: sql.NullString{Valid: false, String: ""},
					},
					{
						Id:   sql.NullInt64{Valid: true, Int64: 2},
						Age:  sql.NullInt64{Valid: true, Int64: 10},
						Name: sql.NullString{Valid: true, String: "zwl"},
					},
					{
						Id:   sql.NullInt64{Valid: true, Int64: 9},
						Age:  sql.NullInt64{Valid: true, Int64: 10},
						Name: sql.NullString{Valid: true, String: "xx"},
					},
					{
						Id:   sql.NullInt64{Valid: true, Int64: 3},
						Age:  sql.NullInt64{Valid: true, Int64: 20},
						Name: sql.NullString{Valid: true, String: "zwl"},
					},
					{
						Id:   sql.NullInt64{Valid: true, Int64: 6},
						Age:  sql.NullInt64{Valid: true, Int64: 20},
						Name: sql.NullString{Valid: true, String: "dm"},
					},
					{
						Id:   sql.NullInt64{Valid: true, Int64: 4},
						Age:  sql.NullInt64{Valid: true, Int64: 20},
						Name: sql.NullString{Valid: false, String: ""},
					},
				}
			}(),
		},
	}
	for _, tc := range testcases {
		ms.T().Run(tc.name, func(t *testing.T) {
			merger, err := NewMerger(false, tc.sortColumns...)
			require.NoError(t, err)
			rows, err := merger.Merge(context.Background(), tc.rowsList())
			require.NoError(t, err)
			res := make([]Nullable, 0, len(tc.wantVal))
			for rows.Next() {
				nullT := Nullable{}
				err := rows.Scan(&nullT.Id, &nullT.Age, &nullT.Name)
				require.NoError(ms.T(), err)
				res = append(res, nullT)
			}
			require.True(t, rows.(*Rows).closed)
			assert.NoError(t, rows.Err())
			assert.Equal(t, tc.wantVal, res)
			tc.afterFunc()
		})
	}
}

type Nullable struct {
	Id   sql.NullInt64
	Age  sql.NullInt64
	Name sql.NullString
}

func TestRows_NextResultSet(t *testing.T) {
	assert.False(t, (&Rows{}).NextResultSet())
}
