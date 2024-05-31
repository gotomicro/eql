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
	ms.mockDB01, ms.mock01, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	ms.mockDB02, ms.mock02, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	ms.mockDB03, ms.mock03, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	ms.mockDB04, ms.mock04, err = sqlmock.New()
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
				aggregator.NewCount(merger.NewColumnInfo(2, "id")),
			},
			GroupByColumns: []merger.ColumnInfo{
				merger.NewColumnInfo(0, "county"),
				merger.NewColumnInfo(1, "gender"),
			},
			rowsList: func() []rows.Rows {
				query := "SELECT `county`,`gender`,SUM(`id`) FROM `t1` GROUP BY `country`,`gender`"
				cols := []string{"county", "gender", "SUM(id)"}
				ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow("hangzhou", "male", 10).AddRow("hangzhou", "female", 20).AddRow("shanghai", "female", 30))
				ms.mock02.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow("shanghai", "male", 40).AddRow("shanghai", "female", 50).AddRow("hangzhou", "female", 60))
				ms.mock03.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow("shanghai", "male", 70).AddRow("shanghai", "female", 80))
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
				aggregator.NewCount(merger.NewColumnInfo(1, "id")),
			},
			GroupByColumns: []merger.ColumnInfo{
				merger.NewColumnInfo(0, "user_name"),
			},
			rowsList: func() []rows.Rows {
				query := "SELECT `user_name`,SUM(`id`) FROM `t1` GROUP BY `user_name`"
				cols := []string{"user_name", "SUM(id)"}
				ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow("zwl", 10).AddRow("dm", 20))
				ms.mock02.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow("xz", 10).AddRow("zwl", 20))
				ms.mock03.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow("dm", 20))
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
				aggregator.NewCount(merger.NewColumnInfo(1, "id")),
			},
			GroupByColumns: []merger.ColumnInfo{
				merger.NewColumnInfo(0, "user_name"),
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
				aggregator.NewCount(merger.NewColumnInfo(1, "id")),
			},
			GroupByColumns: []merger.ColumnInfo{
				merger.NewColumnInfo(0, "user_name"),
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
				aggregator.NewCount(merger.NewColumnInfo(1, "id")),
			},
			GroupByColumns: []merger.ColumnInfo{
				merger.NewColumnInfo(0, "user_name"),
			},
			rowsList: func() []rows.Rows {
				query := "SELECT `user_name`,SUM(`id`) FROM `t1` GROUP BY `user_name`"
				cols := []string{"user_name", "SUM(id)"}
				ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow("zwl", 10).AddRow("dm", 20).RowError(1, nextMockErr))
				ms.mock02.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow("xz", 10).AddRow("zwl", 20))
				ms.mock03.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow("dm", 20))
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
			merger := NewAggregatorMerger(tc.aggregators, tc.GroupByColumns)
			ctx, cancel := tc.ctx()
			groupByRows, err := merger.Merge(ctx, tc.rowsList)
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
				aggregator.NewCount(merger.NewColumnInfo(1, "COUNT(id)")),
			},
			GroupByColumns: []merger.ColumnInfo{
				merger.NewColumnInfo(0, "user_name"),
			},
			rowsList: func() []rows.Rows {
				query := "SELECT `user_name`,COUNT(`id`) FROM `t1` GROUP BY `user_name`"
				cols := []string{"user_name", "SUM(id)"}
				ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow("zwl", 10).AddRow("dm", 20))
				ms.mock02.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow("xz", 10).AddRow("zwl", 20))
				ms.mock03.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow("dm", 20))
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
				aggregator.NewCount(merger.NewColumnInfo(1, "COUNT(id)")),
			},
			GroupByColumns: []merger.ColumnInfo{
				merger.NewColumnInfo(0, "user_name"),
			},
			rowsList: func() []rows.Rows {
				query := "SELECT `user_name`,COUNT(`id`) FROM `t1` GROUP BY `user_name`"
				cols := []string{"user_name", "SUM(id)"}
				ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow("zwl", 10).AddRow("xm", 20))
				ms.mock02.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow("xz", 10).AddRow("xx", 20))
				ms.mock03.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow("dm", 20))
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
				aggregator.NewSum(merger.NewColumnInfo(2, "SUM(id)")),
			},
			GroupByColumns: []merger.ColumnInfo{
				merger.NewColumnInfo(0, "county"),
				merger.NewColumnInfo(1, "gender"),
			},
			rowsList: func() []rows.Rows {
				query := "SELECT `county`,`gender`,SUM(`id`) FROM `t1` GROUP BY `country`,`gender`"
				cols := []string{"county", "gender", "SUM(id)"}
				ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow("hangzhou", "male", 10).AddRow("hangzhou", "female", 20).AddRow("shanghai", "female", 30))
				ms.mock02.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow("shanghai", "male", 40).AddRow("shanghai", "female", 50).AddRow("hangzhou", "female", 60))
				ms.mock03.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow("shanghai", "male", 70).AddRow("shanghai", "female", 80))
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
				aggregator.NewSum(merger.NewColumnInfo(2, "SUM(id)")),
				aggregator.NewAVG(merger.NewColumnInfo(3, "SUM(age)"), merger.NewColumnInfo(4, "COUNT(age)"), "AVG(age)"),
			},
			GroupByColumns: []merger.ColumnInfo{
				merger.NewColumnInfo(0, "county"),
				merger.NewColumnInfo(1, "gender"),
			},

			rowsList: func() []rows.Rows {
				query := "SELECT `county`,`gender`,SUM(`id`),SUM(`age`),COUNT(`age`) FROM `t1` GROUP BY `country`,`gender`"
				cols := []string{"county", "gender", "SUM(id)", "SUM(age)", "COUNT(age)"}
				ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow("hangzhou", "male", 10, 100, 2).AddRow("hangzhou", "female", 20, 120, 3).AddRow("shanghai", "female", 30, 90, 3))
				ms.mock02.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow("shanghai", "male", 40, 120, 5).AddRow("shanghai", "female", 50, 120, 4).AddRow("hangzhou", "female", 60, 150, 3))
				ms.mock03.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow("shanghai", "male", 70, 100, 5).AddRow("shanghai", "female", 80, 150, 5))
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
			merger := NewAggregatorMerger(tc.aggregators, tc.GroupByColumns)
			groupByRows, err := merger.Merge(context.Background(), tc.rowsList)
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
		query := "SELECT userid,SUM(id) FROM `t1`"
		ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(1, 10).AddRow(5, 20))
		r, err := ms.mockDB01.QueryContext(context.Background(), query)
		require.NoError(t, err)
		rowsList := []rows.Rows{r}
		merger := NewAggregatorMerger([]aggregator.Aggregator{aggregator.NewSum(merger.NewColumnInfo(1, "SUM(id)"))}, []merger.ColumnInfo{merger.NewColumnInfo(0, "userid")})
		rows, err := merger.Merge(context.Background(), rowsList)
		require.NoError(t, err)
		userid := 0
		sumId := 0
		err = rows.Scan(&userid, &sumId)
		assert.Equal(t, errs.ErrMergerScanNotNext, err)
	})
	ms.T().Run("迭代过程中发现错误,调用Scan，返回迭代中发现的错误", func(t *testing.T) {
		cols := []string{"userid", "SUM(id)"}
		query := "SELECT userid,SUM(id) FROM `t1`"
		ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(1, 10).AddRow(5, 20))
		r, err := ms.mockDB01.QueryContext(context.Background(), query)
		require.NoError(t, err)
		rowsList := []rows.Rows{r}
		merger := NewAggregatorMerger([]aggregator.Aggregator{&mockAggregate{}}, []merger.ColumnInfo{merger.NewColumnInfo(0, "userid")})
		rows, err := merger.Merge(context.Background(), rowsList)
		require.NoError(t, err)
		userid := 0
		sumId := 0
		rows.Next()
		err = rows.Scan(&userid, &sumId)
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
				query := "SELECT username,COUNT(`id`) FROM `t1`"
				ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow("zwl", 1))
				ms.mock02.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow("daming", 2))
				ms.mock03.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow("wu", 4))
				ms.mock04.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow("ming", 5))
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
				merger.NewColumnInfo(0, "username"),
			},
			wantErr: aggregatorErr,
		},
	}
	for _, tc := range testcases {
		ms.T().Run(tc.name, func(t *testing.T) {
			merger := NewAggregatorMerger(tc.aggregators, tc.GroupByColumns)
			rows, err := merger.Merge(context.Background(), tc.rowsList())
			require.NoError(t, err)
			for rows.Next() {
			}
			count := int64(0)
			name := ""
			err = rows.Scan(&name, &count)
			assert.Equal(t, tc.wantErr, err)
			assert.Equal(t, tc.wantErr, rows.Err())
		})
	}
}

func (ms *MergerSuite) TestAggregatorRows_Columns() {
	cols := []string{"userid", "SUM(grade)", "COUNT(grade)", "SUM(id)", "MIN(id)", "MAX(id)", "COUNT(id)"}
	query := "SELECT SUM(`grade`),COUNT(`grade`),SUM(`id`),MIN(`id`),MAX(`id`),COUNT(`id`),`userid` FROM `t1`"
	ms.mock01.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(1, 1, 2, 1, 3, 10, "zwl"))
	ms.mock02.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(2, 1, 3, 2, 4, 11, "dm"))
	ms.mock03.ExpectQuery("SELECT *").WillReturnRows(sqlmock.NewRows(cols).AddRow(3, 1, 4, 3, 5, 12, "xm"))
	aggregators := []aggregator.Aggregator{
		aggregator.NewAVG(merger.NewColumnInfo(0, "SUM(grade)"), merger.NewColumnInfo(1, "COUNT(grade)"), "AVG(grade)"),
		aggregator.NewSum(merger.NewColumnInfo(2, "SUM(id)")),
		aggregator.NewMin(merger.NewColumnInfo(3, "MIN(id)")),
		aggregator.NewMax(merger.NewColumnInfo(4, "MAX(id)")),
		aggregator.NewCount(merger.NewColumnInfo(5, "COUNT(id)")),
	}
	groupbyColumns := []merger.ColumnInfo{
		merger.NewColumnInfo(6, "userid"),
	}
	merger := NewAggregatorMerger(aggregators, groupbyColumns)
	dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
	rowsList := make([]rows.Rows, 0, len(dbs))
	for _, db := range dbs {
		row, err := db.QueryContext(context.Background(), query)
		require.NoError(ms.T(), err)
		rowsList = append(rowsList, row)
	}

	rows, err := merger.Merge(context.Background(), rowsList)
	require.NoError(ms.T(), err)
	wantCols := []string{"userid", "AVG(grade)", "SUM(id)", "MIN(id)", "MAX(id)", "COUNT(id)"}
	ms.T().Run("Next没有迭代完", func(t *testing.T) {
		for rows.Next() {
			columns, err := rows.Columns()
			require.NoError(t, err)
			assert.Equal(t, wantCols, columns)
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

type mockAggregate struct {
	cols [][]any
}

func (m *mockAggregate) Aggregate(cols [][]any) (any, error) {
	m.cols = cols
	return nil, aggregatorErr
}

func (*mockAggregate) ColumnName() string {
	return "mockAggregate"
}

func TestAggregatorRows_NextResultSet(t *testing.T) {
	assert.False(t, (&AggregatorRows{}).NextResultSet())
}
