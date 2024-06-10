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

package aggregatemerger

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/ecodeclub/eorm/internal/rows"
	_ "github.com/mattn/go-sqlite3"

	"github.com/ecodeclub/eorm/internal/merger"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/ecodeclub/eorm/internal/merger/internal/aggregatemerger/aggregator"
	"github.com/ecodeclub/eorm/internal/merger/internal/errs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/multierr"
)

var (
	nextMockErr   = errors.New("rows: MockNextErr")
	aggregatorErr = errors.New("aggregator: MockAggregatorErr")
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
	db05     *sql.DB
}

func (ms *MergerSuite) SetupTest() {
	t := ms.T()
	ms.initMock(t)
}

func (ms *MergerSuite) TearDownTest() {

	ms.NoError(ms.mock01.ExpectationsWereMet())
	ms.NoError(ms.mock02.ExpectationsWereMet())
	ms.NoError(ms.mock03.ExpectationsWereMet())

	_ = ms.mockDB01.Close()
	_ = ms.mockDB02.Close()
	_ = ms.mockDB03.Close()
	_ = ms.mockDB04.Close()
	_ = ms.db05.Close()
}

func (ms *MergerSuite) initMock(t *testing.T) {
	var err error
	query := "CREATE TABLE t1" +
		"(" +
		"   id INT PRIMARY KEY     NOT NULL," +
		"   grade            INT     NOT NULL" +
		");"
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
	db05, err := sql.Open("sqlite3", "file:test01.db?cache=shared&mode=memory")
	if err != nil {
		t.Fatal(err)
	}
	ms.db05 = db05
	_, err = db05.ExecContext(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
}

func TestMerger(t *testing.T) {
	suite.Run(t, &MergerSuite{})
}

func (ms *MergerSuite) TestRows_NextAndScan() {
	testcases := []struct {
		name        string
		sqlRows     func() []rows.Rows
		wantVal     []any
		aggregators func() []aggregator.Aggregator
		gotVal      []any
		wantErr     error
	}{
		{
			name: "sqlite的ColumnType 使用了多级指针",
			sqlRows: func() []rows.Rows {
				query1 := "insert into `t1` values (1,10),(2,20),(3,30)"
				_, err := ms.db05.ExecContext(context.Background(), query1)
				require.NoError(ms.T(), err)
				cols := []string{"SUM(id)"}
				query := "SELECT SUM(`id`) FROM `t1`"
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(10))
				ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(20))
				ms.mock03.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(30))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03, ms.db05}
				rowsList := make([]rows.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			wantVal: []any{int64(66)},
			gotVal: func() []any {
				return []any{
					0,
				}
			}(),
			aggregators: func() []aggregator.Aggregator {
				return []aggregator.Aggregator{
					aggregator.NewSum(merger.ColumnInfo{Index: 0, Name: "id", AggregateFunc: "SUM"}),
				}
			},
		},
		{
			name: "SUM(id)",
			sqlRows: func() []rows.Rows {
				cols := []string{"SUM(id)"}
				query := "SELECT SUM(`id`) FROM `t1`"
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(10))
				ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(20))
				ms.mock03.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(30))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
				rowsList := make([]rows.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			wantVal: []any{int64(60)},
			gotVal: func() []any {
				return []any{
					0,
				}
			}(),
			aggregators: func() []aggregator.Aggregator {
				return []aggregator.Aggregator{
					aggregator.NewSum(merger.ColumnInfo{Index: 0, Name: "id", AggregateFunc: "SUM"}),
				}
			},
		},

		{
			name: "MAX(id)",
			sqlRows: func() []rows.Rows {
				cols := []string{"MAX(id)"}
				query := "SELECT MAX(`id`) FROM `t1`"
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(10))
				ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(20))
				ms.mock03.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(30))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
				rowsList := make([]rows.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			wantVal: []any{int64(30)},
			gotVal: func() []any {
				return []any{
					0,
				}
			}(),
			aggregators: func() []aggregator.Aggregator {
				return []aggregator.Aggregator{
					aggregator.NewMax(merger.ColumnInfo{Index: 0, Name: "id", AggregateFunc: "MAX"}),
				}
			},
		},
		{
			name: "MIN(id)",
			sqlRows: func() []rows.Rows {
				cols := []string{"MIN(id)"}
				query := "SELECT MIN(`id`) FROM `t1`"
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(10))
				ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(20))
				ms.mock03.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(30))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
				rowsList := make([]rows.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			wantVal: []any{int64(10)},
			gotVal: func() []any {
				return []any{
					0,
				}
			}(),
			aggregators: func() []aggregator.Aggregator {
				return []aggregator.Aggregator{
					aggregator.NewMin(merger.ColumnInfo{Index: 0, Name: "id", AggregateFunc: "MIN"}),
				}
			},
		},
		{
			name: "COUNT(id)",
			sqlRows: func() []rows.Rows {
				cols := []string{"COUNT(id)"}
				query := "SELECT COUNT(`id`) FROM `t1`"
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(10))
				ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(20))
				ms.mock03.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(10))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
				rowsList := make([]rows.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			wantVal: []any{int64(40)},
			gotVal: func() []any {
				return []any{
					0,
				}
			}(),
			aggregators: func() []aggregator.Aggregator {
				return []aggregator.Aggregator{
					aggregator.NewCount(merger.ColumnInfo{Index: 0, Name: "id", AggregateFunc: "COUNT"}),
				}
			},
		},
		{
			name: "AVG(grade)",
			sqlRows: func() []rows.Rows {
				cols := []string{"AVG(`grade`)", "SUM(`grade`)", "COUNT(`grade`)"}
				query := "SELECT AVG(`grade`) AS `avg_grade`, SUM(`grade`),COUNT(`grade`) FROM `t1`"
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(200, 2000, 10))
				ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(100, 2000, 20))
				ms.mock03.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(200, 2000, 10))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
				rowsList := make([]rows.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			wantVal: []any{
				float64(150),
			},
			gotVal: func() []any {
				return []any{
					float64(0),
				}
			}(),
			aggregators: func() []aggregator.Aggregator {
				return []aggregator.Aggregator{
					aggregator.NewAVG(
						merger.ColumnInfo{Index: 0, Name: `grade`, AggregateFunc: "AVG", Alias: "`avg_grade`"},
						merger.ColumnInfo{Index: 1, Name: `grade`, AggregateFunc: "SUM"},
						merger.ColumnInfo{Index: 2, Name: `grade`, AggregateFunc: "COUNT"},
					),
				}
			},
		},
		// 下面为多个聚合函数组合的情况

		// 1.每种聚合函数出现一次
		{
			name: "COUNT(id),MAX(id),MIN(id),SUM(id),AVG(grade)",
			sqlRows: func() []rows.Rows {
				cols := []string{"COUNT(id)", "MAX(id)", "MIN(id)", "SUM(id)", "AVG(`grade`)", "SUM(grade)", "COUNT(grade)"}
				query := "SELECT COUNT(`id`),MAX(`id`),MIN(`id`),SUM(`id`),AVG(`grade`),SUM(`grade`),COUNT(`grade`) FROM `t1`"
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(10, 20, 1, 100, 100, 2000, 20))
				ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(20, 30, 0, 200, 80, 800, 10))
				ms.mock03.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(10, 40, 2, 300, 90, 1800, 20))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
				rowsList := make([]rows.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			wantVal: []any{
				int64(40), int64(40), int64(0), int64(600), float64(4600) / float64(50),
			},
			gotVal: func() []any {
				return []any{
					0, 0, 0, 0, float64(0),
				}
			}(),
			aggregators: func() []aggregator.Aggregator {
				return []aggregator.Aggregator{
					aggregator.NewCount(merger.ColumnInfo{Index: 0, Name: "id", AggregateFunc: "COUNT"}),
					aggregator.NewMax(merger.ColumnInfo{Index: 1, Name: "id", AggregateFunc: "MAX"}),
					aggregator.NewMin(merger.ColumnInfo{Index: 2, Name: "id", AggregateFunc: "MIN"}),
					aggregator.NewSum(merger.ColumnInfo{Index: 3, Name: "id", AggregateFunc: "SUM"}),
					aggregator.NewAVG(
						merger.ColumnInfo{Index: 4, Name: `grade`, AggregateFunc: "AVG"},
						merger.ColumnInfo{Index: 5, Name: `grade`, AggregateFunc: "SUM"},
						merger.ColumnInfo{Index: 6, Name: `grade`, AggregateFunc: "COUNT"},
					),
				}
			},
		},
		// 2. 聚合函数出现一次或多次，会有相同的聚合函数类型，且相同的聚合函数类型会有连续出现，和不连续出现。
		// 两个avg会包含sum列在前，和sum列在后的状态。并且有完全相同的列出现
		{
			name: "AVG(grade),SUM(grade),AVG(grade),MIN(id),MIN(userid),MAX(id),COUNT(id)",
			sqlRows: func() []rows.Rows {
				cols := []string{"AVG(grade)", "SUM(grade)", "COUNT(grade)", "SUM(grade)", "AVG(grade)", "COUNT(grade)", "SUM(grade)", "MIN(id)", "MIN(userid)", "MAX(id)", "COUNT(id)"}
				query := "SELECT AVG(`grade`), SUM(`grade`),COUNT(`grade`),SUM(`grade`),AVG(`grade`),COUNT(`grade`),SUM(`grade`),MIN(`id`),MIN(`userid`),MAX(`id`),COUNT(`id`) FROM `t1`"
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(100, 2000, 20, 2000, 100, 20, 2000, 10, 20, 200, 200))
				ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(100, 1000, 10, 1000, 100, 10, 1000, 20, 30, 300, 300))
				ms.mock03.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(80, 800, 10, 800, 80, 10, 800, 5, 6, 100, 200))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
				rowsList := make([]rows.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			wantVal: []any{
				float64(3800) / float64(40), int64(3800), float64(3800) / float64(40), int64(5), int64(6), int64(300), int64(700),
			},
			gotVal: func() []any {
				return []any{
					float64(0), 0, float64(0), 0, 0, 0, 0,
				}
			}(),
			aggregators: func() []aggregator.Aggregator {
				return []aggregator.Aggregator{
					aggregator.NewAVG(
						merger.ColumnInfo{Index: 0, Name: `grade`, AggregateFunc: "AVG"},
						merger.ColumnInfo{Index: 1, Name: `grade`, AggregateFunc: "SUM"},
						merger.ColumnInfo{Index: 2, Name: `grade`, AggregateFunc: "COUNT"},
					),
					aggregator.NewSum(merger.ColumnInfo{Index: 3, Name: "grade", AggregateFunc: "SUM"}),
					aggregator.NewAVG(
						merger.ColumnInfo{Index: 4, Name: `grade`, AggregateFunc: "AVG"},
						merger.ColumnInfo{Index: 6, Name: `grade`, AggregateFunc: "SUM"},
						merger.ColumnInfo{Index: 5, Name: `grade`, AggregateFunc: "COUNT"},
					),
					aggregator.NewMin(merger.ColumnInfo{Index: 7, Name: "id", AggregateFunc: "MIN"}),
					aggregator.NewMin(merger.ColumnInfo{Index: 8, Name: "userid", AggregateFunc: "MIN"}),
					aggregator.NewMax(merger.ColumnInfo{Index: 9, Name: "id", AggregateFunc: "MAX"}),
					aggregator.NewCount(merger.ColumnInfo{Index: 10, Name: "id", AggregateFunc: "COUNT"}),
				}
			},
		},

		// 下面为RowList为有元素返回的行数为空

		// 1. Rows 列表中有一个Rows返回行数为空，在前面会返回错误
		{
			name: "RowsList有一个Rows为空，在前面",
			sqlRows: func() []rows.Rows {
				cols := []string{"SUM(id)"}
				query := "SELECT SUM(`id`) FROM `t1`"
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(10))
				ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(20))
				ms.mock03.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(30))
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
			wantVal: []any{60},
			gotVal: func() []any {
				return []any{
					0,
				}
			}(),
			wantErr: errs.ErrMergerAggregateHasEmptyRows,
			aggregators: func() []aggregator.Aggregator {
				return []aggregator.Aggregator{
					aggregator.NewSum(merger.ColumnInfo{Index: 0, Name: "id", AggregateFunc: "SUM"}),
				}
			},
		},
		// 2. Rows 列表中有一个Rows返回行数为空，在中间会返回错误
		{
			name: "RowsList有一个Rows为空，在中间",
			sqlRows: func() []rows.Rows {
				cols := []string{"SUM(id)"}
				query := "SELECT SUM(`id`) FROM `t1`"
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(10))
				ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(20))
				ms.mock03.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(30))
				ms.mock04.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB04, ms.mockDB02, ms.mockDB03}
				rowsList := make([]rows.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			wantVal: []any{60},
			gotVal: func() []any {
				return []any{
					0,
				}
			}(),
			wantErr: errs.ErrMergerAggregateHasEmptyRows,
			aggregators: func() []aggregator.Aggregator {
				return []aggregator.Aggregator{
					aggregator.NewSum(merger.ColumnInfo{Index: 0, Name: "id", AggregateFunc: "SUM"}),
				}
			},
		},
		// 3. Rows 列表中有一个Rows返回行数为空，在后面会返回错误
		{
			name: "RowsList有一个Rows为空，在最后",
			sqlRows: func() []rows.Rows {
				cols := []string{"SUM(id)"}
				query := "SELECT SUM(`id`) FROM `t1`"
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(10))
				ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(20))
				ms.mock03.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(30))
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
			wantVal: []any{60},
			gotVal: func() []any {
				return []any{
					0,
				}
			}(),
			wantErr: errs.ErrMergerAggregateHasEmptyRows,
			aggregators: func() []aggregator.Aggregator {
				return []aggregator.Aggregator{
					aggregator.NewSum(merger.ColumnInfo{Index: 0, Name: "id", AggregateFunc: "SUM"}),
				}
			},
		},
		// 4. Rows 列表中全部Rows返回的行数为空，不会返回错误
		{
			name: "RowsList全部为空",
			sqlRows: func() []rows.Rows {
				cols := []string{"SUM(id)"}
				query := "SELECT SUM(`id`) FROM `t1`"
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
			wantErr: errs.ErrMergerAggregateHasEmptyRows,
			aggregators: func() []aggregator.Aggregator {
				return []aggregator.Aggregator{
					aggregator.NewSum(merger.ColumnInfo{Index: 0, Name: "id", AggregateFunc: "SUM"}),
				}
			},
		},
	}
	for _, tc := range testcases {
		ms.T().Run(tc.name, func(t *testing.T) {
			m := NewMerger(tc.aggregators()...)
			r, err := m.Merge(context.Background(), tc.sqlRows())
			require.NoError(t, err)
			for r.Next() {
				kk := make([]any, 0, len(tc.gotVal))
				for i := 0; i < len(tc.gotVal); i++ {
					kk = append(kk, &tc.gotVal[i])
				}
				err = r.Scan(kk...)
				require.NoError(t, err)
			}
			assert.Equal(t, tc.wantErr, r.Err())
			if r.Err() != nil {
				return
			}
			assert.Equal(t, tc.wantVal, tc.gotVal)
		})
	}
}

func (ms *MergerSuite) TestRows_NextAndErr() {
	testcases := []struct {
		name        string
		rowsList    func() []rows.Rows
		wantErr     error
		aggregators []aggregator.Aggregator
	}{
		{
			name: "sqlRows列表中有一个返回error",
			rowsList: func() []rows.Rows {
				cols := []string{"COUNT(id)"}
				query := "SELECT COUNT(`id`) FROM `t1`"
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(1))
				ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(2))
				ms.mock03.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(4).RowError(0, nextMockErr))
				ms.mock04.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(5))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03, ms.mockDB04}
				rowsList := make([]rows.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			aggregators: func() []aggregator.Aggregator {
				return []aggregator.Aggregator{
					aggregator.NewCount(merger.ColumnInfo{Index: 0, Name: "id", AggregateFunc: "COUNT"}),
				}
			}(),
			wantErr: nextMockErr,
		},
		{
			name: "有一个aggregator返回error",
			rowsList: func() []rows.Rows {
				cols := []string{"COUNT(id)"}
				query := "SELECT COUNT(`id`) FROM `t1`"
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(1))
				ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(2))
				ms.mock03.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(4))
				ms.mock04.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(5))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03, ms.mockDB04}
				rowsList := make([]rows.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			aggregators: func() []aggregator.Aggregator {
				return []aggregator.Aggregator{
					&mockAggregate{},
				}
			}(),
			wantErr: aggregatorErr,
		},
	}
	for _, tc := range testcases {
		ms.T().Run(tc.name, func(t *testing.T) {
			m := NewMerger(tc.aggregators...)
			r, err := m.Merge(context.Background(), tc.rowsList())
			require.NoError(t, err)
			for r.Next() {
			}
			count := int64(0)
			err = r.Scan(&count)
			assert.Equal(t, tc.wantErr, err)
			assert.Equal(t, tc.wantErr, r.Err())
		})
	}
}

func (ms *MergerSuite) TestRows_Close() {
	cols := []string{"SUM(id)"}
	targetSQL := "SELECT SUM(`id`) FROM `t1`"
	ms.mock01.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(1))
	ms.mock02.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(2).CloseError(newCloseMockErr("db02")))
	ms.mock03.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(3).CloseError(newCloseMockErr("db03")))
	m := NewMerger(aggregator.NewSum(merger.ColumnInfo{Index: 0, Name: "id", AggregateFunc: "SUM"}))
	dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
	rowsList := make([]rows.Rows, 0, len(dbs))
	for _, db := range dbs {
		row, err := db.QueryContext(context.Background(), targetSQL)
		require.NoError(ms.T(), err)
		rowsList = append(rowsList, row)
	}
	r, err := m.Merge(context.Background(), rowsList)
	require.NoError(ms.T(), err)
	// 判断当前是可以正常读取的
	require.True(ms.T(), r.Next())
	var id int
	err = r.Scan(&id)
	require.NoError(ms.T(), err)
	err = r.Close()
	ms.T().Run("close返回multiError", func(t *testing.T) {
		assert.Equal(ms.T(), multierr.Combine(newCloseMockErr("db02"), newCloseMockErr("db03")), err)
	})
	ms.T().Run("close之后Next返回false", func(t *testing.T) {
		for i := 0; i < len(rowsList); i++ {
			require.False(ms.T(), rowsList[i].Next())
		}
		require.False(ms.T(), r.Next())
	})
	ms.T().Run("close之后Scan返回迭代过程中的错误", func(t *testing.T) {
		var id int
		err := r.Scan(&id)
		assert.Equal(t, errs.ErrMergerRowsClosed, err)
	})
	ms.T().Run("close之后调用Columns方法返回错误", func(t *testing.T) {
		_, err := r.Columns()
		require.Error(t, err)
	})
	ms.T().Run("close多次是等效的", func(t *testing.T) {
		for i := 0; i < 4; i++ {
			err = r.Close()
			require.NoError(t, err)
		}
	})
}

func (ms *MergerSuite) TestRows_Columns() {
	cols := []string{"AVG(grade)", "SUM(grade)", "COUNT(grade)", "SUM(id)", "MIN(id)", "MAX(id)", "COUNT(id)"}
	query := "SELECT AVG(`grade`), SUM(`grade`),COUNT(`grade`),SUM(`id`),MIN(`id`),MAX(`id`),COUNT(`id`) FROM `t1`"
	ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(1, 1, 1, 2, 1, 3, 10))
	ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(2, 2, 1, 3, 2, 4, 11))
	ms.mock03.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(3, 3, 1, 4, 3, 5, 12))
	aggregators := []aggregator.Aggregator{
		aggregator.NewAVG(
			merger.ColumnInfo{Index: 0, Name: `grade`, AggregateFunc: "AVG"},
			merger.ColumnInfo{Index: 1, Name: `grade`, AggregateFunc: "SUM"},
			merger.ColumnInfo{Index: 2, Name: `grade`, AggregateFunc: "COUNT"},
		),
		aggregator.NewSum(merger.ColumnInfo{Index: 3, Name: "id", AggregateFunc: "SUM"}),
		aggregator.NewMin(merger.ColumnInfo{Index: 4, Name: "id", AggregateFunc: "MIN"}),
		aggregator.NewMax(merger.ColumnInfo{Index: 5, Name: "id", AggregateFunc: "MAX"}),
		aggregator.NewCount(merger.ColumnInfo{Index: 6, Name: "id", AggregateFunc: "COUNT"}),
	}
	m := NewMerger(aggregators...)
	dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
	rowsList := make([]rows.Rows, 0, len(dbs))
	for _, db := range dbs {
		row, err := db.QueryContext(context.Background(), query)
		require.NoError(ms.T(), err)
		rowsList = append(rowsList, row)
	}

	r, err := m.Merge(context.Background(), rowsList)
	require.NoError(ms.T(), err)
	wantCols := []string{"AVG(grade)", "SUM(id)", "MIN(id)", "MAX(id)", "COUNT(id)"}
	ms.T().Run("Next没有迭代完", func(t *testing.T) {
		for r.Next() {
			columns, err := r.Columns()
			require.NoError(t, err)
			assert.Equal(t, wantCols, columns)
		}
		require.NoError(t, r.Err())
	})
	ms.T().Run("Next迭代完", func(t *testing.T) {
		require.False(t, r.Next())
		require.NoError(t, r.Err())
		_, err := r.Columns()
		assert.Equal(t, errs.ErrMergerRowsClosed, err)
	})
}

func (ms *MergerSuite) TestMerger_Merge() {
	testcases := []struct {
		name    string
		merger  func() *Merger
		ctx     func() (context.Context, context.CancelFunc)
		wantErr error
		sqlRows func() []rows.Rows
	}{
		{
			name: "超时",
			merger: func() *Merger {
				return NewMerger(aggregator.NewSum(merger.ColumnInfo{Index: 0, Name: "id", AggregateFunc: "SUM"}))
			},
			ctx: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithTimeout(context.Background(), 0)
				return ctx, cancel
			},
			wantErr: context.DeadlineExceeded,
			sqlRows: func() []rows.Rows {
				query := "SELECT  SUM(`id`) FROM `t1`;"
				cols := []string{"SUM(id)"}
				res := make([]rows.Rows, 0, 1)
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(1))
				r, _ := ms.mockDB01.QueryContext(context.Background(), query)
				res = append(res, r)
				return res
			},
		},
		{
			name: "sqlRows列表元素个数为0",
			merger: func() *Merger {
				return NewMerger(aggregator.NewSum(merger.ColumnInfo{Index: 0, Name: "id", AggregateFunc: "SUM"}))
			},
			ctx: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				return ctx, cancel
			},
			wantErr: errs.ErrMergerEmptyRows,
			sqlRows: func() []rows.Rows {
				return []rows.Rows{}
			},
		},
		{
			name: "sqlRows列表有nil",
			merger: func() *Merger {
				return NewMerger(aggregator.NewSum(merger.ColumnInfo{Index: 0, Name: "id", AggregateFunc: "SUM"}))
			},
			ctx: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				return ctx, cancel
			},
			wantErr: errs.ErrMergerRowsIsNull,
			sqlRows: func() []rows.Rows {
				return []rows.Rows{nil}
			},
		},
	}
	for _, tc := range testcases {
		ms.T().Run(tc.name, func(t *testing.T) {
			ctx, cancel := tc.ctx()
			m := tc.merger()
			r, err := m.Merge(ctx, tc.sqlRows())
			cancel()
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			require.NotNil(t, r)
		})
	}
}

func (ms *MergerSuite) TestMerger_ColumnTypes() {
	t := ms.T()

	tests := []struct {
		sql            string
		before         func(t *testing.T, sql string) ([]rows.Rows, []string)
		columns        []aggregator.Aggregator
		requireErrFunc require.ErrorAssertionFunc
		after          func(t *testing.T, r rows.Rows, expectedColumnNames []string)
	}{
		{
			sql: "SELECT SUM(`grade`) FROM `t1`",
			before: func(t *testing.T, sql string) ([]rows.Rows, []string) {
				t.Helper()
				targetSQL := sql
				cols := []string{"SUM(`grade`)"}
				ms.mock01.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(400))
				ms.mock02.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(120))
				ms.mock03.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(80))
				return getResultSet(t, targetSQL, ms.mockDB01, ms.mockDB02, ms.mockDB03), cols

			},
			columns: []aggregator.Aggregator{
				aggregator.NewSum(
					merger.ColumnInfo{Index: 0, Name: "`grade`", AggregateFunc: "SUM"},
				),
			},
			requireErrFunc: require.NoError,
			after: func(t *testing.T, r rows.Rows, _ []string) {
				t.Helper()

				expectedColumnNames := []string{"SUM(`grade`)"}
				columns, err := r.Columns()
				require.NoError(t, err)
				require.Equal(t, expectedColumnNames, columns)

				types, err := r.ColumnTypes()
				require.NoError(t, err)

				names := make([]string, 0, len(types))
				for _, typ := range types {
					names = append(names, typ.Name())
				}
				require.Equal(t, expectedColumnNames, names)

				scanFunc := func(rr rows.Rows, valSet *[]any) error {
					var sumGrade int
					if err := rr.Scan(&sumGrade); err != nil {
						return err
					}
					*valSet = append(*valSet, []any{sumGrade})
					return nil
				}

				require.Equal(t, []any{
					[]any{600},
				}, getRowValues(t, r, scanFunc))
			},
		},
		{
			sql: "SELECT AVG(`grade`) AS `avg_grade` FROM `t1`",
			before: func(t *testing.T, sql string) ([]rows.Rows, []string) {
				t.Helper()
				targetSQL := "SELECT AVG(`grade`) AS `avg_grade`, SUM(`grade`), COUNT(`grade`) FROM `t1`"
				cols := []string{"`avg_grade`", "SUM(`grade`)", "COUNT(`grade`)"}
				ms.mock01.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(200, 400, 2))
				ms.mock02.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(40, 120, 3))
				ms.mock03.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(80, 80, 1))
				return getResultSet(t, targetSQL, ms.mockDB01, ms.mockDB02, ms.mockDB03), cols

			},
			columns: []aggregator.Aggregator{
				aggregator.NewAVG(
					merger.ColumnInfo{Index: 0, Name: "`grade`", AggregateFunc: "AVG", Alias: "`avg_grade`"},
					merger.ColumnInfo{Index: 1, Name: "`grade`", AggregateFunc: "SUM"},
					merger.ColumnInfo{Index: 2, Name: "`grade`", AggregateFunc: "COUNT"},
				),
			},
			requireErrFunc: require.NoError,
			after: func(t *testing.T, r rows.Rows, _ []string) {
				t.Helper()

				expectedColumnNames := []string{"`avg_grade`"}
				columns, err := r.Columns()
				require.NoError(t, err)
				require.Equal(t, expectedColumnNames, columns)

				types, err := r.ColumnTypes()
				require.NoError(t, err)

				names := make([]string, 0, len(types))
				for _, typ := range types {
					names = append(names, typ.Name())
				}
				require.Equal(t, expectedColumnNames, names)

				scanFunc := func(rr rows.Rows, valSet *[]any) error {
					var avgGrade float64
					if err := rr.Scan(&avgGrade); err != nil {
						return err
					}
					*valSet = append(*valSet, []any{avgGrade})
					return nil
				}

				cnt := 6
				sum := 600
				require.Equal(t, []any{
					[]any{float64(sum) / float64(cnt)},
				}, getRowValues(t, r, scanFunc))
			},
		},
		{
			sql: "SELECT AVG(`grade`) AS `avg_grade`, AVG(`age`), AVG(`height`) FROM `t1`",
			before: func(t *testing.T, sql string) ([]rows.Rows, []string) {
				t.Helper()
				targetSQL := "SELECT AVG(`grade`) AS `avg_grade`, SUM(`grade`), COUNT(`grade`), AVG(`age`), SUM(`age`), COUNT(`age`) , AVG(`height`), SUM(`height`), COUNT(`height`)  FROM `t1`"
				cols := []string{"`avg_grade`", "SUM(`grade`)", "COUNT(`grade`)", "AVG(`age`)", "SUM(`age`)", "COUNT(`age`)", "AVG(`height`)", "SUM(`height`)", "COUNT(`height`)"}
				ms.mock01.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(200, 400, 2, 18, 36, 2, 160, 320, 2))
				ms.mock02.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(40, 120, 3, 18, 54, 3, 170, 510, 3))
				ms.mock03.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(80, 80, 1, 18, 18, 1, 180, 180, 1))
				return getResultSet(t, targetSQL, ms.mockDB01, ms.mockDB02, ms.mockDB03), cols
			},
			columns: []aggregator.Aggregator{
				aggregator.NewAVG(
					merger.ColumnInfo{Index: 0, Name: "`grade`", AggregateFunc: "AVG", Alias: "`avg_grade`"},
					merger.ColumnInfo{Index: 1, Name: "`grade`", AggregateFunc: "SUM"},
					merger.ColumnInfo{Index: 2, Name: "`grade`", AggregateFunc: "COUNT"},
				),
				aggregator.NewAVG(
					merger.ColumnInfo{Index: 3, Name: "`age`", AggregateFunc: "AVG"},
					merger.ColumnInfo{Index: 4, Name: "`age`", AggregateFunc: "SUM"},
					merger.ColumnInfo{Index: 5, Name: "`age`", AggregateFunc: "COUNT"},
				),
				aggregator.NewAVG(
					merger.ColumnInfo{Index: 6, Name: "`height`", AggregateFunc: "AVG"},
					merger.ColumnInfo{Index: 7, Name: "`height`", AggregateFunc: "SUM"},
					merger.ColumnInfo{Index: 8, Name: "`height`", AggregateFunc: "COUNT"},
				),
			},
			requireErrFunc: require.NoError,
			after: func(t *testing.T, r rows.Rows, _ []string) {
				t.Helper()

				expectedColumnNames := []string{"`avg_grade`", "AVG(`age`)", "AVG(`height`)"}
				columns, err := r.Columns()
				require.NoError(t, err)
				require.Equal(t, expectedColumnNames, columns)

				types, err := r.ColumnTypes()
				require.NoError(t, err)

				names := make([]string, 0, len(types))
				for _, typ := range types {
					names = append(names, typ.Name())
				}
				require.Equal(t, expectedColumnNames, names)

				scanFunc := func(rr rows.Rows, valSet *[]any) error {
					var avgGrade, avgAge, avgHeight float64
					if err := rr.Scan(&avgGrade, &avgAge, &avgHeight); err != nil {
						return err
					}
					*valSet = append(*valSet, []any{avgGrade, avgAge, avgHeight})
					return nil
				}

				require.Equal(t, []any{
					[]any{float64(100), float64(18), float64(1010) / float64(6)},
				}, getRowValues(t, r, scanFunc))
			},
		},
		{
			sql: "SELECT AVG(`grade`),AVG(`age`), AVG(`height`) FROM `t1`",
			before: func(t *testing.T, sql string) ([]rows.Rows, []string) {
				t.Helper()
				targetSQL := "SELECT AVG(`grade`), SUM(`grade`), COUNT(`grade`), AVG(`age`), SUM(`age`), COUNT(`age`) , AVG(`height`), SUM(`height`), COUNT(`height`)  FROM `t1`"
				cols := []string{"AVG(`grade`)", "SUM(`grade`)", "COUNT(`grade`)", "AVG(`age`)", "SUM(`age`)", "COUNT(`age`)", "AVG(`height`)", "SUM(`height`)", "COUNT(`height`)"}
				ms.mock01.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(200, 400, 2, 18, 36, 2, 160, 320, 2))
				ms.mock02.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(40, 120, 3, 18, 54, 3, 170, 510, 3))
				ms.mock03.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(80, 80, 1, 18, 18, 1, 180, 180, 1))
				return getResultSet(t, targetSQL, ms.mockDB01, ms.mockDB02, ms.mockDB03), cols
			},
			columns: []aggregator.Aggregator{
				aggregator.NewAVG(
					merger.ColumnInfo{Index: 0, Name: "`grade`", AggregateFunc: "AVG"},
					merger.ColumnInfo{Index: 1, Name: "`grade`", AggregateFunc: "SUM"},
					merger.ColumnInfo{Index: 2, Name: "`grade`", AggregateFunc: "COUNT"},
				),
				aggregator.NewAVG(
					merger.ColumnInfo{Index: 3, Name: "`age`", AggregateFunc: "AVG"},
					merger.ColumnInfo{Index: 4, Name: "`age`", AggregateFunc: "SUM"},
					merger.ColumnInfo{Index: 5, Name: "`age`", AggregateFunc: "COUNT"},
				),
				aggregator.NewAVG(
					merger.ColumnInfo{Index: 6, Name: "`height`", AggregateFunc: "AVG"},
					merger.ColumnInfo{Index: 7, Name: "`height`", AggregateFunc: "SUM"},
					merger.ColumnInfo{Index: 8, Name: "`height`", AggregateFunc: "COUNT"},
				),
			},
			requireErrFunc: require.NoError,
			after: func(t *testing.T, r rows.Rows, _ []string) {
				t.Helper()

				expectedColumnNames := []string{"AVG(`grade`)", "AVG(`age`)", "AVG(`height`)"}
				columns, err := r.Columns()
				require.NoError(t, err)
				require.Equal(t, expectedColumnNames, columns)

				types, err := r.ColumnTypes()
				require.NoError(t, err)

				names := make([]string, 0, len(types))
				for _, typ := range types {
					names = append(names, typ.Name())
				}
				require.Equal(t, expectedColumnNames, names)

				require.NoError(t, r.Close())

				_, err = r.ColumnTypes()
				require.ErrorIs(t, err, errs.ErrMergerRowsClosed)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			res, cols := tt.before(t, tt.sql)
			m := NewMerger(tt.columns...)
			r, err := m.Merge(context.Background(), res)
			require.NoError(t, err)
			tt.after(t, r, cols)
		})
	}
}

type mockAggregate struct {
	cols [][]any
}

func (m *mockAggregate) Aggregate(cols [][]any) (any, error) {
	m.cols = cols
	return nil, aggregatorErr
}

func (*mockAggregate) ColumnInfo() merger.ColumnInfo {
	return merger.ColumnInfo{Name: "mockAggregateColumn"}
}

func (*mockAggregate) Name() string {
	return "mockAggregate"
}

func TestRows_NextResultSet(t *testing.T) {
	assert.False(t, (&Rows{}).NextResultSet())
}

func getRowValues(t *testing.T, r rows.Rows, scanFunc func(r rows.Rows, valSet *[]any) error) []any {
	var res []any
	for r.Next() {
		require.NoError(t, scanFunc(r, &res))
	}
	return res
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
