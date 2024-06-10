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
	"errors"
	"log"
	"testing"

	"github.com/ecodeclub/eorm/internal/rows"

	"github.com/ecodeclub/eorm/internal/merger"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/ecodeclub/eorm/internal/merger/internal/aggregatemerger/aggregator"
	"github.com/ecodeclub/eorm/internal/merger/internal/errs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

var (
	nextMockErr   = errors.New("rows: MockNextErr")
	aggregatorErr = errors.New("aggregator: MockAggregatorErr")
)

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

func TestMerger(t *testing.T) {
	suite.Run(t, &MergerSuite{})
}

func (ms *MergerSuite) TestAggregatorMerger_Merge() {
	testcases := []struct {
		name           string
		aggregators    []aggregator.Aggregator
		rowsList       []rows.Rows
		GroupByColumns []merger.ColumnInfo
		wantErr        error
		ctx            func() (context.Context, context.CancelFunc)
	}{
		{
			name: "正常案例",
			aggregators: []aggregator.Aggregator{
				aggregator.NewCount(merger.ColumnInfo{Index: 2, Name: "id", AggregateFunc: "COUNT"}),
			},
			GroupByColumns: []merger.ColumnInfo{
				{Index: 0, Name: "county"},
				{Index: 1, Name: "gender"},
			},
			rowsList: func() []rows.Rows {
				query := "SELECT `county`,`gender`,SUM(`id`) FROM `t1` GROUP BY `country`,`gender`"
				cols := []string{"county", "gender", "SUM(id)"}
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow("hangzhou", "male", 10).AddRow("hangzhou", "female", 20).AddRow("shanghai", "female", 30))
				ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow("shanghai", "male", 40).AddRow("shanghai", "female", 50).AddRow("hangzhou", "female", 60))
				ms.mock03.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow("shanghai", "male", 70).AddRow("shanghai", "female", 80))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
				rowsList := make([]rows.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				log.Printf("rows = %#v\n", rowsList)
				return rowsList
			}(),

			ctx: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				return ctx, cancel
			},
		},
		{
			name: "超时",
			aggregators: []aggregator.Aggregator{
				aggregator.NewCount(merger.ColumnInfo{Index: 1, Name: "id", AggregateFunc: "COUNT"}),
			},
			GroupByColumns: []merger.ColumnInfo{
				{Index: 0, Name: "user_name"},
			},
			rowsList: func() []rows.Rows {
				query := "SELECT `user_name`,SUM(`id`) FROM `t1` GROUP BY `user_name`"
				cols := []string{"user_name", "SUM(id)"}
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow("zwl", 10).AddRow("dm", 20))
				ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow("xz", 10).AddRow("zwl", 20))
				ms.mock03.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow("dm", 20))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
				rowsList := make([]rows.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			}(),
			ctx: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithTimeout(context.Background(), 0)
				return ctx, cancel
			},
			wantErr: context.DeadlineExceeded,
		},
		{
			name: "rowsList为空",
			aggregators: []aggregator.Aggregator{
				aggregator.NewCount(merger.ColumnInfo{Index: 1, Name: "id", AggregateFunc: "COUNT"}),
			},
			GroupByColumns: []merger.ColumnInfo{
				{
					Index: 0,
					Name:  "user_name",
				},
			},
			rowsList: func() []rows.Rows {
				return []rows.Rows{}
			}(),
			ctx: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				return ctx, cancel
			},
			wantErr: errs.ErrMergerEmptyRows,
		},
		{
			name: "rowsList中有nil",
			aggregators: []aggregator.Aggregator{
				aggregator.NewCount(merger.ColumnInfo{Index: 1, Name: "id", AggregateFunc: "COUNT"}),
			},
			GroupByColumns: []merger.ColumnInfo{
				{Index: 0, Name: "user_name"},
			},
			rowsList: func() []rows.Rows {
				return []rows.Rows{nil}
			}(),
			ctx: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				return ctx, cancel
			},
			wantErr: errs.ErrMergerRowsIsNull,
		},
		{
			name: "rowsList中有sql.Rows返回错误",
			aggregators: []aggregator.Aggregator{
				aggregator.NewCount(merger.ColumnInfo{Index: 1, Name: "id", AggregateFunc: "COUNT"}),
			},
			GroupByColumns: []merger.ColumnInfo{
				{Index: 0, Name: "user_name"},
			},
			rowsList: func() []rows.Rows {
				query := "SELECT `user_name`,SUM(`id`) FROM `t1` GROUP BY `user_name`"
				cols := []string{"user_name", "SUM(id)"}
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow("zwl", 10).AddRow("dm", 20).RowError(1, nextMockErr))
				ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow("xz", 10).AddRow("zwl", 20))
				ms.mock03.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow("dm", 20))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
				rowsList := make([]rows.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			}(),
			ctx: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				return ctx, cancel
			},
			wantErr: nextMockErr,
		},
	}
	for _, tc := range testcases {
		ms.T().Run(tc.name, func(t *testing.T) {
			m := NewAggregatorMerger(tc.aggregators, tc.GroupByColumns)
			ctx, cancel := tc.ctx()
			groupByRows, err := m.Merge(ctx, tc.rowsList)
			cancel()
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			require.NotNil(t, groupByRows)
		})
	}
}

func (ms *MergerSuite) TestAggregatorRows_NextAndScan() {
	testcases := []struct {
		name           string
		aggregators    []aggregator.Aggregator
		rowsList       []rows.Rows
		wantVal        [][]any
		gotVal         [][]any
		GroupByColumns []merger.ColumnInfo
		wantErr        error
	}{
		{
			name: "同一组数据在不同的sql.Rows中",
			aggregators: []aggregator.Aggregator{
				aggregator.NewCount(merger.ColumnInfo{Index: 1, Name: "id", AggregateFunc: "COUNT"}),
			},
			GroupByColumns: []merger.ColumnInfo{
				{Index: 0, Name: "user_name"},
			},
			rowsList: func() []rows.Rows {
				query := "SELECT `user_name`,COUNT(`id`) FROM `t1` GROUP BY `user_name`"
				cols := []string{"user_name", "SUM(id)"}
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow("zwl", 10).AddRow("dm", 20))
				ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow("xz", 10).AddRow("zwl", 20))
				ms.mock03.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow("dm", 20))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
				rowsList := make([]rows.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			}(),
			wantVal: [][]any{
				{"zwl", int64(30)},
				{"dm", int64(40)},
				{"xz", int64(10)},
			},
			gotVal: [][]any{
				{"", int64(0)},
				{"", int64(0)},
				{"", int64(0)},
			},
		},
		{
			name: "同一组数据在同一个sql.Rows中",
			aggregators: []aggregator.Aggregator{
				aggregator.NewCount(merger.ColumnInfo{Index: 1, Name: "id", AggregateFunc: "COUNT"}),
			},
			GroupByColumns: []merger.ColumnInfo{
				{Index: 0, Name: "user_name"},
			},
			rowsList: func() []rows.Rows {
				query := "SELECT `user_name`,COUNT(`id`) FROM `t1` GROUP BY `user_name`"
				cols := []string{"user_name", "SUM(id)"}
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow("zwl", 10).AddRow("xm", 20))
				ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow("xz", 10).AddRow("xx", 20))
				ms.mock03.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow("dm", 20))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
				rowsList := make([]rows.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			}(),
			wantVal: [][]any{
				{"zwl", int64(10)},
				{"xm", int64(20)},
				{"xz", int64(10)},
				{"xx", int64(20)},
				{"dm", int64(20)},
			},
			gotVal: [][]any{
				{"", int64(0)},
				{"", int64(0)},
				{"", int64(0)},
				{"", int64(0)},
				{"", int64(0)},
			},
		},
		{
			name: "多个分组列",
			aggregators: []aggregator.Aggregator{
				aggregator.NewSum(merger.ColumnInfo{Index: 2, Name: "id", AggregateFunc: "SUM"}),
			},
			GroupByColumns: []merger.ColumnInfo{
				{Index: 0, Name: "county"},
				{Index: 1, Name: "gender"},
			},
			rowsList: func() []rows.Rows {
				query := "SELECT `county`,`gender`,SUM(`id`) FROM `t1` GROUP BY `country`,`gender`"
				cols := []string{"county", "gender", "SUM(id)"}
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow("hangzhou", "male", 10).AddRow("hangzhou", "female", 20).AddRow("shanghai", "female", 30))
				ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow("shanghai", "male", 40).AddRow("shanghai", "female", 50).AddRow("hangzhou", "female", 60))
				ms.mock03.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow("shanghai", "male", 70).AddRow("shanghai", "female", 80))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
				rowsList := make([]rows.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			}(),
			wantVal: [][]any{
				{
					"hangzhou",
					"male",
					int64(10),
				},
				{
					"hangzhou",
					"female",
					int64(80),
				},
				{
					"shanghai",
					"female",
					int64(160),
				},
				{
					"shanghai",
					"male",
					int64(110),
				},
			},
			gotVal: [][]any{
				{"", "", int64(0)},
				{"", "", int64(0)},
				{"", "", int64(0)},
				{"", "", int64(0)},
			},
		},
		{
			name: "多个聚合函数",
			aggregators: []aggregator.Aggregator{
				aggregator.NewSum(merger.ColumnInfo{Index: 2, Name: "id", AggregateFunc: "SUM"}),
				aggregator.NewAVG(
					merger.ColumnInfo{Index: 3, Name: "age", AggregateFunc: "AVG"},
					merger.ColumnInfo{Index: 4, Name: "age", AggregateFunc: "SUM"},
					merger.ColumnInfo{Index: 5, Name: "age", AggregateFunc: "COUNT"},
				),
			},
			GroupByColumns: []merger.ColumnInfo{
				{Index: 0, Name: "county"},
				{Index: 1, Name: "gender"},
			},

			rowsList: func() []rows.Rows {
				query := "SELECT `county`,`gender`,SUM(`id`), AVG(`age`), SUM(`age`),COUNT(`age`) FROM `t1` GROUP BY `country`,`gender`"
				cols := []string{"county", "gender", "SUM(id)", "AVG(`age`)", "SUM(age)", "COUNT(age)"}
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow("hangzhou", "male", 10, 50, 100, 2).AddRow("hangzhou", "female", 20, 40, 120, 3).AddRow("shanghai", "female", 30, 30, 90, 3))
				ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow("shanghai", "male", 40, 24, 120, 5).AddRow("shanghai", "female", 50, 30, 120, 4).AddRow("hangzhou", "female", 60, 50, 150, 3))
				ms.mock03.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow("shanghai", "male", 70, 20, 100, 5).AddRow("shanghai", "female", 80, 30, 150, 5))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
				rowsList := make([]rows.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			}(),
			wantVal: [][]any{
				{
					"hangzhou",
					"male",
					int64(10),
					float64(50),
				},
				{
					"hangzhou",
					"female",
					int64(80),
					float64(45),
				},
				{
					"shanghai",
					"female",
					int64(160),
					float64(30),
				},
				{
					"shanghai",
					"male",
					int64(110),
					float64(22),
				},
			},
			gotVal: [][]any{
				{"", "", int64(0), float64(0)},
				{"", "", int64(0), float64(0)},
				{"", "", int64(0), float64(0)},
				{"", "", int64(0), float64(0)},
			},
		},
	}
	for _, tc := range testcases {
		ms.T().Run(tc.name, func(t *testing.T) {
			m := NewAggregatorMerger(tc.aggregators, tc.GroupByColumns)
			groupByRows, err := m.Merge(context.Background(), tc.rowsList)
			require.NoError(t, err)

			idx := 0
			for groupByRows.Next() {
				if idx >= len(tc.gotVal) {
					break
				}
				tmp := make([]any, 0, len(tc.gotVal[0]))
				for i := range tc.gotVal[idx] {
					tmp = append(tmp, &tc.gotVal[idx][i])
				}
				err := groupByRows.Scan(tmp...)
				require.NoError(t, err)
				idx++
			}
			require.NoError(t, groupByRows.Err())
			assert.Equal(t, tc.wantVal, tc.gotVal)
		})
	}
}

func (ms *MergerSuite) TestAggregatorRows_ScanAndErr() {
	ms.T().Run("未调用Next，直接Scan，返回错", func(t *testing.T) {
		cols := []string{"userid", "SUM(id)"}
		query := "SELECT `userid`,SUM(`id`) FROM `t1`"
		ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(1, 10).AddRow(5, 20))
		sqlRows, err := ms.mockDB01.QueryContext(context.Background(), query)
		require.NoError(t, err)
		rowsList := []rows.Rows{sqlRows}
		m := NewAggregatorMerger([]aggregator.Aggregator{
			aggregator.NewSum(merger.ColumnInfo{Index: 1, Name: "id", AggregateFunc: "SUM"})},
			[]merger.ColumnInfo{{Index: 0, Name: "userid"}})
		r, err := m.Merge(context.Background(), rowsList)
		require.NoError(t, err)
		userid := 0
		sumId := 0
		err = r.Scan(&userid, &sumId)
		assert.Equal(t, errs.ErrMergerScanNotNext, err)
	})
	ms.T().Run("迭代过程中发现错误,调用Scan，返回迭代中发现的错误", func(t *testing.T) {
		cols := []string{"userid", "SUM(id)"}
		query := "SELECT `userid`,SUM(`id`) FROM `t1`"
		ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(1, 10).AddRow(5, 20))
		sqlRows, err := ms.mockDB01.QueryContext(context.Background(), query)
		require.NoError(t, err)
		rowsList := []rows.Rows{sqlRows}
		m := NewAggregatorMerger([]aggregator.Aggregator{&mockAggregate{}}, []merger.ColumnInfo{{Index: 0, Name: "userid"}})
		r, err := m.Merge(context.Background(), rowsList)
		require.NoError(t, err)
		userid := 0
		sumId := 0
		r.Next()
		err = r.Scan(&userid, &sumId)
		assert.Equal(t, aggregatorErr, err)
	})

}

func (ms *MergerSuite) TestAggregatorRows_NextAndErr() {
	testcases := []struct {
		name           string
		rowsList       func() []rows.Rows
		wantErr        error
		aggregators    []aggregator.Aggregator
		GroupByColumns []merger.ColumnInfo
	}{
		{
			name: "有一个aggregator返回error",
			rowsList: func() []rows.Rows {
				cols := []string{"username", "COUNT(id)"}
				query := "SELECT `username`,COUNT(`id`) FROM `t1`"
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow("zwl", 1))
				ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow("david", 2))
				ms.mock03.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow("wu", 4))
				ms.mock04.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow("ming", 5))
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
			GroupByColumns: []merger.ColumnInfo{
				{Index: 0, Name: "username"},
			},
			wantErr: aggregatorErr,
		},
	}
	for _, tc := range testcases {
		ms.T().Run(tc.name, func(t *testing.T) {
			m := NewAggregatorMerger(tc.aggregators, tc.GroupByColumns)
			r, err := m.Merge(context.Background(), tc.rowsList())
			require.NoError(t, err)
			for r.Next() {
			}
			count := int64(0)
			name := ""
			err = r.Scan(&name, &count)
			assert.Equal(t, tc.wantErr, err)
			assert.Equal(t, tc.wantErr, r.Err())
		})
	}
}

func (ms *MergerSuite) TestAggregatorRows_Columns() {
	cols := []string{"user_id", "AVG(`grade`)", "SUM(grade)", "COUNT(grade)", "SUM(grade)", "MIN(grade)", "MAX(grade)", "COUNT(grade)"}
	query := "SELECT `user_id`, AVG(`grade`), SUM(`grade`),COUNT(`grade`),SUM(`grade`),MIN(`grade`),MAX(`grade`),COUNT(`grade`) FROM `t1` GROUP BY`user_id`"
	ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(1, 50, 150, 3, 150, 30, 100, 3))
	ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(2, 100, 200, 2, 200, 50, 150, 2))
	ms.mock03.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(3, 150, 1, 1, 150, 150, 150, 1))
	aggregators := []aggregator.Aggregator{
		aggregator.NewAVG(
			merger.ColumnInfo{Index: 1, Name: "grade", AggregateFunc: "AVG"},
			merger.ColumnInfo{Index: 2, Name: "grade", AggregateFunc: "SUM"},
			merger.ColumnInfo{Index: 3, Name: "grade", AggregateFunc: "COUNT"},
		),
		aggregator.NewSum(merger.ColumnInfo{Index: 4, Name: "grade", AggregateFunc: "SUM"}),
		aggregator.NewMin(merger.ColumnInfo{Index: 5, Name: "grade", AggregateFunc: "MIN"}),
		aggregator.NewMax(merger.ColumnInfo{Index: 6, Name: "grade", AggregateFunc: "MAX"}),
		aggregator.NewCount(merger.ColumnInfo{Index: 7, Name: "grade", AggregateFunc: "COUNT"}),
	}
	groupByColumns := []merger.ColumnInfo{
		{Index: 0, Name: "user_id"},
	}
	m := NewAggregatorMerger(aggregators, groupByColumns)
	dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
	rowsList := make([]rows.Rows, 0, len(dbs))
	for _, db := range dbs {
		row, err := db.QueryContext(context.Background(), query)
		require.NoError(ms.T(), err)
		rowsList = append(rowsList, row)
	}

	r, err := m.Merge(context.Background(), rowsList)
	require.NoError(ms.T(), err)
	wantCols := []string{"user_id", "AVG(grade)", "SUM(grade)", "MIN(grade)", "MAX(grade)", "COUNT(grade)"}
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

func (ms *MergerSuite) TestRows_ColumnTypes() {
	t := ms.T()

	query := "SELECT AVG(`grade`) AS `avg_grade` FROM `t1`"
	cols := []string{"`avg_grade`", "SUM(`grade`)", "COUNT(`grade`)"}
	ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(100, 200, 2))
	ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(90, 270, 3))
	ms.mock03.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(110, 440, 4))
	aggregators := []aggregator.Aggregator{
		aggregator.NewAVG(
			merger.ColumnInfo{Index: 0, Name: "`grade`", AggregateFunc: "AVG", Alias: "`avg_grade`"},
			merger.ColumnInfo{Index: 1, Name: "`grade`", AggregateFunc: "SUM"},
			merger.ColumnInfo{Index: 2, Name: "`grade`", AggregateFunc: "COUNT"},
		),
	}

	groupByColumns := []merger.ColumnInfo{
		{
			Index:         0,
			Name:          "`grade`",
			AggregateFunc: "AVG",
			Alias:         "`avg_grade`",
		},
	}
	r, err := NewAggregatorMerger(aggregators, groupByColumns).Merge(context.Background(), getResultSet(t, query, ms.mockDB01, ms.mockDB02, ms.mockDB03))
	require.NoError(t, err)

	t.Run("rows未关闭", func(t *testing.T) {
		types, err := r.ColumnTypes()
		require.NoError(t, err)

		names := make([]string, 0, len(types))
		for _, typ := range types {
			names = append(names, typ.Name())
		}
		require.Equal(t, []string{"`avg_grade`"}, names)
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

type mockAggregate struct {
	cols [][]any
}

func (m *mockAggregate) Aggregate(cols [][]any) (any, error) {
	m.cols = cols
	return nil, aggregatorErr
}

func (*mockAggregate) ColumnInfo() merger.ColumnInfo {
	return merger.ColumnInfo{Name: "mockAggregate"}
}

func (*mockAggregate) Name() string {
	return "mockAggregate"
}

func TestAggregatorRows_NextResultSet(t *testing.T) {
	assert.False(t, (&AggregatorRows{}).NextResultSet())
}
