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

package batchmerger

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/ecodeclub/eorm/internal/rows"

	"go.uber.org/multierr"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/ecodeclub/eorm/internal/merger/internal/errs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

var (
	nextMockErr = errors.New("rows: MockNextErr")
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

func (ms *MergerSuite) TestMerger_Merge() {
	testcases := []struct {
		name     string
		rowsList func() []rows.Rows
		ctx      func() (context.Context, context.CancelFunc)
		wantErr  error
	}{
		{
			name: "sql.Rows列表中没有元素",
			rowsList: func() []rows.Rows {
				return []rows.Rows{}
			},
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			wantErr: errs.ErrMergerEmptyRows,
		},
		{
			name: "sql.Rows列表中有元素为nil",
			rowsList: func() []rows.Rows {
				return []rows.Rows{nil}
			},
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			wantErr: errs.ErrMergerRowsIsNull,
		},
		{
			name: "sqlRows字段不同_少一个字段",
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			rowsList: func() []rows.Rows {
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
			name: "sqlRows字段不同",
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			rowsList: func() []rows.Rows {
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
			name: "正常的案例",
			rowsList: func() []rows.Rows {
				query := "SELECT * FROM `t1`"
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows([]string{"id", "name", "address"}).AddRow(1, "abex", "cn").AddRow(5, "bruce", "cn"))
				ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows([]string{"id", "name", "address"}).AddRow(3, "alex", "cn").AddRow(4, "x", "cn"))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02}
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
		},
		{
			name: "超时",
			ctx: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithTimeout(context.Background(), 0)
				return ctx, cancel
			},
			wantErr: context.DeadlineExceeded,
			rowsList: func() []rows.Rows {
				query := "SELECT * FROM `t1`;"
				cols := []string{"id"}
				res := make([]rows.Rows, 0, 1)
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(1))
				rows, _ := ms.mockDB01.QueryContext(context.Background(), query)
				res = append(res, rows)
				return res
			},
		},
	}
	for _, tc := range testcases {
		ms.T().Run(tc.name, func(t *testing.T) {
			merger := NewMerger()
			ctx, cancel := tc.ctx()
			rows, err := merger.Merge(ctx, tc.rowsList())
			cancel()
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			require.NotNil(t, rows)
		})
	}
}

func (ms *MergerSuite) TestRows_NextAndScan() {

	testCases := []struct {
		name    string
		sqlRows func() []rows.Rows
		wantVal []string
		wantErr error
		scanErr error
	}{
		{
			name: "sqlRows列表中没有空行",
			sqlRows: func() []rows.Rows {
				query := "SELECT * FROM `t1`;"
				cols := []string{"id"}
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow("1").AddRow("2"))
				ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow("2").AddRow("2"))
				ms.mock03.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow("3").AddRow("4"))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
				res := make([]rows.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, _ := db.QueryContext(context.Background(), query)
					res = append(res, row)
				}
				return res
			},
			wantVal: []string{"1", "2", "2", "2", "3", "4"},
		},
		{
			name: "sqlRows列表中，在前面有一个sqlRows返回空行在前面",
			sqlRows: func() []rows.Rows {
				query := "SELECT * FROM `t1`;"
				cols := []string{"id"}
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow("1"))
				ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow("2"))
				ms.mock03.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow("3").AddRow("4"))
				ms.mock04.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols))
				dbs := []*sql.DB{ms.mockDB04, ms.mockDB01, ms.mockDB02, ms.mockDB03}
				res := make([]rows.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, _ := db.QueryContext(context.Background(), query)
					res = append(res, row)
				}
				return res
			},
			wantVal: []string{"1", "2", "3", "4"},
		},
		{
			name: "sqlRows列表中，在前面有多个sqlRows返回空行",
			sqlRows: func() []rows.Rows {
				query := "SELECT * FROM `t1`;"
				cols := []string{"id"}
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols))
				ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow("2"))
				ms.mock03.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow("3").AddRow("4"))
				ms.mock04.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols))
				dbs := []*sql.DB{ms.mockDB04, ms.mockDB01, ms.mockDB02, ms.mockDB03}
				res := make([]rows.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, _ := db.QueryContext(context.Background(), query)
					res = append(res, row)
				}
				return res
			},
			wantVal: []string{"2", "3", "4"},
		},
		{
			name: "sqlRows列表中，在中间有一个sqlRows返回空行",
			sqlRows: func() []rows.Rows {
				query := "SELECT * FROM `t1`;"
				cols := []string{"id"}
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols))
				ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow("2"))
				ms.mock03.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow("3").AddRow("4"))
				dbs := []*sql.DB{ms.mockDB02, ms.mockDB01, ms.mockDB03}
				res := make([]rows.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, _ := db.QueryContext(context.Background(), query)
					res = append(res, row)
				}
				return res
			},
			wantVal: []string{"2", "3", "4"},
		},
		{
			name: "sqlRows列表中，在中间有多个sqlRows返回空行",
			sqlRows: func() []rows.Rows {
				query := "SELECT * FROM `t1`;"
				cols := []string{"id"}
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols))
				ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow("2"))
				ms.mock03.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow("3").AddRow("4"))
				ms.mock04.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols))
				dbs := []*sql.DB{ms.mockDB02, ms.mockDB01, ms.mockDB04, ms.mockDB03}
				res := make([]rows.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, _ := db.QueryContext(context.Background(), query)
					res = append(res, row)
				}
				return res
			},
			wantVal: []string{"2", "3", "4"},
		},
		{
			name: "sqlRows列表中，在后面有一个sqlRows返回空行",
			sqlRows: func() []rows.Rows {
				query := "SELECT * FROM `t1`;"
				cols := []string{"id"}
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols))
				ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow("2"))
				ms.mock03.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow("3").AddRow("4"))
				dbs := []*sql.DB{ms.mockDB02, ms.mockDB03, ms.mockDB01}
				res := make([]rows.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, _ := db.QueryContext(context.Background(), query)
					res = append(res, row)
				}
				return res
			},
			wantVal: []string{"2", "3", "4"},
		},
		{
			name: "sqlRows列表中，在后面有多个个sqlRows返回空行",
			sqlRows: func() []rows.Rows {
				query := "SELECT * FROM `t1`;"
				cols := []string{"id"}
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols))
				ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow("2"))
				ms.mock03.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow("3").AddRow("4"))
				ms.mock04.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols))
				dbs := []*sql.DB{ms.mockDB02, ms.mockDB03, ms.mockDB01, ms.mockDB04}
				res := make([]rows.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, _ := db.QueryContext(context.Background(), query)
					res = append(res, row)
				}
				return res
			},
			wantVal: []string{"2", "3", "4"},
		},
		{
			name: "sqlRows列表中的元素均返回空行",
			sqlRows: func() []rows.Rows {
				query := "SELECT * FROM `t1`;"
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows([]string{"id"}))
				ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows([]string{"id"}))
				ms.mock03.ExpectQuery(query).WillReturnRows(sqlmock.NewRows([]string{"id"}))
				res := make([]rows.Rows, 0, 3)
				row01, _ := ms.mockDB01.QueryContext(context.Background(), query)
				res = append(res, row01)
				row02, _ := ms.mockDB02.QueryContext(context.Background(), query)
				res = append(res, row02)
				row03, _ := ms.mockDB03.QueryContext(context.Background(), query)
				res = append(res, row03)
				return res
			},
			wantVal: []string{},
		},
	}
	for _, tc := range testCases {
		ms.T().Run(tc.name, func(t *testing.T) {
			merger := NewMerger()
			rows, err := merger.Merge(context.Background(), tc.sqlRows())
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			res := make([]string, 0, 4)
			for rows.Next() {
				var id string
				err = rows.Scan(&id)
				assert.Equal(t, tc.scanErr, err)
				if err != nil {
					return
				}
				res = append(res, id)
			}
			require.NoError(t, rows.Err())
			assert.Equal(t, tc.wantVal, res)
		})
	}

}

func (ms *MergerSuite) TestRows_NextAndErr() {
	testcases := []struct {
		name     string
		rowsList func() []rows.Rows
		wantErr  error
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
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB04, ms.mockDB03}
				rowsList := make([]rows.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			wantErr: nextMockErr,
		},
	}
	for _, tc := range testcases {
		ms.T().Run(tc.name, func(t *testing.T) {
			merger := NewMerger()
			rows, err := merger.Merge(context.Background(), tc.rowsList())
			require.NoError(t, err)
			for rows.Next() {
			}
			assert.Equal(t, tc.wantErr, rows.Err())
		})
	}
}

func (ms *MergerSuite) TestRows_ScanAndErr() {
	ms.T().Run("未调用Next，直接Scan，返回错", func(t *testing.T) {
		cols := []string{"id"}
		query := "SELECT * FROM `t1`"
		ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(1).AddRow(5))
		r, err := ms.mockDB01.QueryContext(context.Background(), query)
		require.NoError(t, err)
		rowsList := []rows.Rows{r}
		merger := NewMerger()
		rows, err := merger.Merge(context.Background(), rowsList)
		require.NoError(t, err)
		id := 0
		err = rows.Scan(&id)
		require.Error(t, err)
	})
	ms.T().Run("迭代过程中发现错误,调用Scan，返回迭代中发现的错误", func(t *testing.T) {
		cols := []string{"id"}
		query := "SELECT * FROM `t1`"
		ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(1).RowError(0, nextMockErr))
		r, err := ms.mockDB01.QueryContext(context.Background(), query)
		require.NoError(t, err)
		rowsList := []rows.Rows{r}
		merger := NewMerger()
		rows, err := merger.Merge(context.Background(), rowsList)
		require.NoError(t, err)
		for rows.Next() {
		}
		id := 0
		err = rows.Scan(&id)
		assert.Equal(t, nextMockErr, err)
	})

}

func (ms *MergerSuite) TestRows_Close() {
	cols := []string{"id"}
	query := "SELECT * FROM `t1`"
	ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow("1"))
	ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow("2").CloseError(newCloseMockErr("db02")))
	ms.mock03.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow("3").AddRow("4").CloseError(newCloseMockErr("db03")))
	merger := NewMerger()
	dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
	rowsList := make([]rows.Rows, 0, len(dbs))
	for _, db := range dbs {
		row, err := db.QueryContext(context.Background(), query)
		require.NoError(ms.T(), err)
		rowsList = append(rowsList, row)
	}
	rs, err := merger.Merge(context.Background(), rowsList)
	require.NoError(ms.T(), err)
	// 判断当前是可以正常读取的
	require.True(ms.T(), rs.Next())
	var id int
	err = rs.Scan(&id)
	require.NoError(ms.T(), err)
	err = rs.Close()
	ms.T().Run("close返回multierror", func(t *testing.T) {
		assert.Equal(ms.T(), multierr.Combine(newCloseMockErr("db02"), newCloseMockErr("db03")), err)
	})
	ms.T().Run("close之后Next返回false", func(t *testing.T) {
		for i := 0; i < len(rowsList); i++ {
			require.False(ms.T(), rowsList[i].Next())
		}
		require.False(ms.T(), rs.Next())
	})
	ms.T().Run("close之后Scan返回迭代过程中的错误", func(t *testing.T) {
		var id int
		err := rs.Scan(&id)
		assert.Equal(t, errs.ErrMergerRowsClosed, err)
	})
	ms.T().Run("close之后调用Columns方法返回错误", func(t *testing.T) {
		_, err := rs.Columns()
		require.Error(t, err)
	})
	ms.T().Run("close多次是等效的", func(t *testing.T) {
		for i := 0; i < 4; i++ {
			err = rs.Close()
			require.NoError(t, err)
		}
	})
}

func (ms *MergerSuite) TestRows_Columns() {
	cols := []string{"id"}
	query := "SELECT * FROM `t1`"
	ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow("1"))
	ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow("2"))
	ms.mock03.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow("3").AddRow("4"))
	merger := NewMerger()
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

func (ms *MergerSuite) TestRows_ColumnTypes() {
	t := ms.T()

	cols := []string{"id"}
	query := "SELECT * FROM `t1`"
	ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow("1"))
	ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow("2"))
	ms.mock03.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow("3").AddRow("4"))
	r, err := NewMerger().Merge(context.Background(), getResultSet(t, query, ms.mockDB01, ms.mockDB02, ms.mockDB03))
	require.NoError(t, err)

	t.Run("rows未关闭", func(t *testing.T) {
		types, err := r.ColumnTypes()
		require.NoError(t, err)

		names := make([]string, 0, len(types))
		for _, typ := range types {
			names = append(names, typ.Name())
		}
		require.Equal(t, cols, names)
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

func TestMerger(t *testing.T) {
	suite.Run(t, &MergerSuite{})
}

func TestRows_NextResultSet(t *testing.T) {
	assert.False(t, (&Rows{}).NextResultSet())
}
