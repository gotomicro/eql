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

package factory

import (
	"context"
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/ecodeclub/eorm/internal/merger"
	"github.com/ecodeclub/eorm/internal/merger/internal/aggregatemerger"
	"github.com/ecodeclub/eorm/internal/merger/internal/batchmerger"
	"github.com/ecodeclub/eorm/internal/merger/internal/groupbymerger"
	"github.com/ecodeclub/eorm/internal/merger/internal/pagedmerger"
	"github.com/ecodeclub/eorm/internal/merger/internal/sortmerger"
	"github.com/ecodeclub/eorm/internal/query"
	"github.com/ecodeclub/eorm/internal/rows"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

func TestNew(t *testing.T) {
	t.Skip()
	// TODO: 本测试为列探索测试用例,以后会删掉
	tests := []struct {
		name string
		sql  string
		spec QuerySpec

		wantMergers    []merger.Merger
		requireErrFunc require.ErrorAssertionFunc
	}{
		// 单一特征的测试用例
		{
			name: "无特征_使用批量合并",
			sql:  "SELECT `id`,`status` FROM `orders`",
			spec: QuerySpec{
				Features: nil,
				Select: []merger.ColumnInfo{
					{
						Index: 0,
						Name:  "id",
					},
					{
						Index: 1,
						Name:  "status",
					},
				},
			},
			wantMergers: []merger.Merger{
				&batchmerger.Merger{},
			},
			requireErrFunc: require.NoError,
		},
		{
			name: "SELECT中有别名_使用批量合并",
			sql:  "SELECT `id` AS `order_id`, `user_id` AS `uid`, `order_sn` AS `sn`, `amount`, `status`, COUNT(*) AS `total_orders`, SUM(`amount`) AS `total_amount`, AVG(`amount`) AS `avg_amount` FROM `orders` WHERE (`status` = 1 AND `amount` > 100) OR `amount` > 1000;",
			spec: QuerySpec{
				Features: nil,
				Select: []merger.ColumnInfo{
					{
						Index: 0,
						Name:  "id",
					},
					{
						Index: 1,
						Name:  "status",
					},
				},
			},
			wantMergers: []merger.Merger{
				&batchmerger.Merger{},
			},
			requireErrFunc: require.NoError,
		},
		// SELECT中有聚合函数_使用
		{
			name: "有聚合函数_使用聚合合并",
			sql:  "SELECT COUNT(`id`) FROM `orders`",
			spec: QuerySpec{
				Features: []query.Feature{query.AggregateFunc},
				// TODO: 初始化aggregatemerger时,要从select中读取参数
				Select: []merger.ColumnInfo{
					{
						Index: 0,
						Name:  "COUNT(id)",
					},
				},
			},
			wantMergers: []merger.Merger{
				&aggregatemerger.Merger{},
			},
			requireErrFunc: require.NoError,
		},
		{
			name: "有GroupBy_无Having_GroupBy无分片键_使用分组聚合合并",
			sql:  "SELECT `amount` FROM `orders` GROUP BY `amount`",
			spec: QuerySpec{
				Features: []query.Feature{query.GroupBy},
				GroupBy:  []merger.ColumnInfo{{Name: "amount"}},
			},
			wantMergers: []merger.Merger{
				&groupbymerger.AggregatorMerger{},
			},
			requireErrFunc: require.NoError,
		},
		{
			name: "有GroupBy_无Having_GroupBy中有分片键_使用分组聚合合并",
			sql:  "SELECT AVG(`amount`) FROM `orders` GROUP BY `buyer_id`",
			spec: QuerySpec{
				Features: []query.Feature{query.GroupBy},
				Select: []merger.ColumnInfo{
					{
						Index:         0,
						Name:          "AVG(`amount`)",
						AggregateFunc: "AVG", // isAggregateFunc ?
					},
				},
				// TOTO: GroupBy
				GroupBy: []merger.ColumnInfo{{Name: "buyer_id"}},
			},
			wantMergers: []merger.Merger{
				&groupbymerger.AggregatorMerger{},
			},
			requireErrFunc: require.NoError,
		},
		{
			name: "OrderBy",
			sql:  "SELECT `sn` FROM `orders` ORDER BY `amount`",
			spec: QuerySpec{
				Features: []query.Feature{query.OrderBy},
				Select: []merger.ColumnInfo{
					{
						Index: 0,
						Name:  "sn",
					},
				},
				OrderBy: []merger.ColumnInfo{
					{
						Index: 0, // 索引排序? amount没有出现在SELECT子句,出现在orderBy子句中
						Name:  "amount",
						Order: merger.ASC,
					},
				},
			},
			wantMergers: []merger.Merger{
				&sortmerger.Merger{},
			},
			requireErrFunc: require.NoError,
		},
		{
			name: "Limit",
			sql:  "SELECT `name` FROM `orders` LIMIT 10",
			spec: QuerySpec{
				Features: []query.Feature{query.Limit},
				// Select:   []merger.ColumnInfo{{Index: 0, Name: "name"}},
				Limit: 10,
			},
			wantMergers: []merger.Merger{
				&pagedmerger.Merger{},
			},
			requireErrFunc: require.NoError,
		},
		// 组合特征的测试用例
		{
			name: "AggregateFunc_GroupBy",
			sql:  "SELECT `amount`, COUNT(*) FROM `orders` GROUP BY `amount`",
			spec: QuerySpec{
				Features: []query.Feature{query.AggregateFunc, query.GroupBy},
				Select:   []merger.ColumnInfo{{Name: "amount"}, {Name: "COUNT(*)"}},
				GroupBy:  []merger.ColumnInfo{{Name: "amount"}},
			},
			wantMergers: []merger.Merger{
				&aggregatemerger.Merger{},
				&groupbymerger.AggregatorMerger{},
			},
			requireErrFunc: require.NoError,
		},
		{
			name: "AggregateFunc_OrderBy",
			sql:  "SELECT COUNT(*) FROM `orders` ORDER BY `amount`",
			spec: QuerySpec{
				Features: []query.Feature{query.AggregateFunc, query.OrderBy},
				Select:   []merger.ColumnInfo{{Name: "COUNT(*)"}},
				OrderBy:  []merger.ColumnInfo{{Name: "amount"}},
			},
			wantMergers: []merger.Merger{
				&aggregatemerger.Merger{},
				&sortmerger.Merger{},
			},
			requireErrFunc: require.NoError,
		},
		{
			name: "AggregateFunc_Limit",
			sql:  "SELECT COUNT(*) FROM `orders` LIMIT 10",
			spec: QuerySpec{
				Features: []query.Feature{query.AggregateFunc, query.Limit},
				Select:   []merger.ColumnInfo{{Name: "COUNT(*)"}},
				Limit:    10,
			},
			wantMergers: []merger.Merger{
				// &aggregatemerger.Merger{},
				&pagedmerger.Merger{},
			},
			requireErrFunc: require.NoError,
		},
		{
			name: "GroupBy_OrderBy",
			sql:  "SELECT `amount` FROM `orders` GROUP BY `amount` ORDER BY `amount`",
			spec: QuerySpec{
				Features: []query.Feature{query.GroupBy, query.OrderBy},
				GroupBy:  []merger.ColumnInfo{{Name: "amount"}},
				OrderBy:  []merger.ColumnInfo{{Name: "amount"}},
			},
			wantMergers: []merger.Merger{
				&groupbymerger.AggregatorMerger{},
				&sortmerger.Merger{},
			},
			requireErrFunc: require.NoError,
		},
		{
			name: "GroupBy_Limit",
			sql:  "SELECT `amount` FROM `orders` GROUP BY `amount` LIMIT 10",
			spec: QuerySpec{
				Features: []query.Feature{query.GroupBy, query.Limit},
				GroupBy:  []merger.ColumnInfo{{Name: "amount"}},
				Limit:    10,
			},
			wantMergers: []merger.Merger{
				// &groupbymerger.AggregatorMerger{},
				&pagedmerger.Merger{},
			},
			requireErrFunc: require.NoError,
		},
		{
			name: "OrderBy_Limit",
			sql:  "SELECT `name` FROM `orders` ORDER BY `amount` LIMIT 10",
			spec: QuerySpec{
				Features: []query.Feature{query.OrderBy, query.Limit},
				OrderBy:  []merger.ColumnInfo{{Name: "amount"}},
				Limit:    10,
			},
			wantMergers: []merger.Merger{
				// &sortmerger.Merger{},
				&pagedmerger.Merger{},
			},
			requireErrFunc: require.NoError,
		},
		{
			name: "AggregateFunc_GroupBy_OrderBy",
			sql:  "SELECT `amount`, COUNT(*) FROM `orders` GROUP BY `amount` ORDER BY COUNT(*)",
			spec: QuerySpec{
				Features: []query.Feature{query.AggregateFunc, query.GroupBy, query.OrderBy},
				Select:   []merger.ColumnInfo{{Name: "amount"}, {Name: "COUNT(*)"}},
				GroupBy:  []merger.ColumnInfo{{Name: "amount"}},
				OrderBy:  []merger.ColumnInfo{{Name: "COUNT(*)"}},
			},
			wantMergers: []merger.Merger{
				&aggregatemerger.Merger{},
				&groupbymerger.AggregatorMerger{},
				&sortmerger.Merger{},
			},
			requireErrFunc: require.NoError,
		},
		{
			name: "AggregateFunc_GroupBy_Limit",
			sql:  "SELECT `amount`, COUNT(*) FROM `orders` GROUP BY `amount` LIMIT 10",
			spec: QuerySpec{
				Features: []query.Feature{query.AggregateFunc, query.GroupBy, query.Limit},
				Select:   []merger.ColumnInfo{{Name: "amount"}, {Name: "COUNT(*)"}},
				GroupBy:  []merger.ColumnInfo{{Name: "amount"}},
				Limit:    10,
			},
			wantMergers: []merger.Merger{
				&aggregatemerger.Merger{},
				// &groupbymerger.AggregatorMerger{},
				&pagedmerger.Merger{},
			},
			requireErrFunc: require.NoError,
		},
		{
			name: "AggregateFunc_OrderBy_Limit",
			sql:  "SELECT COUNT(*) FROM `orders` ORDER BY `amount` LIMIT 10",
			spec: QuerySpec{
				Features: []query.Feature{query.AggregateFunc, query.OrderBy, query.Limit},
				Select:   []merger.ColumnInfo{{Name: "COUNT(*)"}},
				OrderBy:  []merger.ColumnInfo{{Name: "amount"}},
				Limit:    10,
			},
			wantMergers: []merger.Merger{
				&aggregatemerger.Merger{},
				// &sortmerger.Merger{},
				&pagedmerger.Merger{},
			},
			requireErrFunc: require.NoError,
		},
		{
			name: "GroupBy_OrderBy_Limit",
			sql:  "SELECT `amount` FROM `orders` GROUP BY `amount` ORDER BY `amount` LIMIT 10",
			spec: QuerySpec{
				Features: []query.Feature{query.GroupBy, query.OrderBy, query.Limit},
				GroupBy:  []merger.ColumnInfo{{Name: "amount"}},
				OrderBy:  []merger.ColumnInfo{{Name: "amount"}},
				Limit:    10,
			},
			wantMergers: []merger.Merger{
				&groupbymerger.AggregatorMerger{},
				// &sortmerger.Merger{},
				&pagedmerger.Merger{},
			},
			requireErrFunc: require.NoError,
		},
		{
			name: "AggregateFunc_GroupBy_OrderBy_Limit",
			sql:  "SELECT `amount`, COUNT(*) FROM `orders` GROUP BY `amount` ORDER BY COUNT(*) LIMIT 10",
			spec: QuerySpec{
				Features: []query.Feature{query.AggregateFunc, query.GroupBy, query.OrderBy, query.Limit},
				Select:   []merger.ColumnInfo{{Name: "amount"}, {Name: "COUNT(*)"}},
				GroupBy:  []merger.ColumnInfo{{Name: "amount"}},
				OrderBy:  []merger.ColumnInfo{{Name: "COUNT(*)"}},
				Limit:    10,
			},
			wantMergers: []merger.Merger{
				&aggregatemerger.Merger{},
				&groupbymerger.AggregatorMerger{},
				// &sortmerger.Merger{},
				&pagedmerger.Merger{},
			},
			requireErrFunc: require.NoError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			m, err := New(tt.spec, tt.spec)
			tt.requireErrFunc(t, err)

			mp, ok := m.(*MergerPipeline)
			require.True(t, ok)

			// Ensure the number of mergers match
			assert.Equal(t, len(tt.wantMergers), len(mp.mergers))

			// Ensure each merger matches the expected order and type
			for i, expectedMerger := range tt.wantMergers {
				switch expectedMerger.(type) {
				case *batchmerger.Merger:
					assert.IsType(t, &batchmerger.Merger{}, mp.mergers[i])
				case *aggregatemerger.Merger:
					assert.IsType(t, &aggregatemerger.Merger{}, mp.mergers[i])
				case *groupbymerger.AggregatorMerger:
					assert.IsType(t, &groupbymerger.AggregatorMerger{}, mp.mergers[i])
				case *sortmerger.Merger:
					assert.IsType(t, &sortmerger.Merger{}, mp.mergers[i])
				case *pagedmerger.Merger:
					assert.IsType(t, &pagedmerger.Merger{}, mp.mergers[i])
				}
			}
		})
	}
}

func TestFactory(t *testing.T) {
	suite.Run(t, &factoryTestSuite{})
}

type factoryTestSuite struct {
	suite.Suite
	db01   *sql.DB
	mock01 sqlmock.Sqlmock
	db02   *sql.DB
	mock02 sqlmock.Sqlmock
	db03   *sql.DB
	mock03 sqlmock.Sqlmock
}

func (s *factoryTestSuite) SetupTest() {
	var err error
	s.db01, s.mock01, err = sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	s.NoError(err)

	s.db02, s.mock02, err = sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	s.NoError(err)

	s.db03, s.mock03, err = sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	s.NoError(err)
}

func (s *factoryTestSuite) TearDownTest() {
	s.NoError(s.mock01.ExpectationsWereMet())
	s.NoError(s.mock02.ExpectationsWereMet())
	s.NoError(s.mock03.ExpectationsWereMet())
}

func (s *factoryTestSuite) TestSELECT() {
	t := s.T()

	tests := []struct {
		sql            string
		before         func(t *testing.T, sql string) ([]rows.Rows, []string)
		originSpec     QuerySpec
		targetSpec     QuerySpec
		requireErrFunc require.ErrorAssertionFunc
		after          func(t *testing.T, rows rows.Rows, expectedColumnNames []string)
	}{
		// SELECT
		{
			sql: "应该报错_QuerySpec.Select列为空",
			before: func(t *testing.T, sql string) ([]rows.Rows, []string) {
				return nil, nil
			},
			originSpec: QuerySpec{},
			targetSpec: QuerySpec{},
			requireErrFunc: func(t require.TestingT, err error, i ...interface{}) {
				require.ErrorIs(t, err, ErrEmptyColumnList)
			},
			after: func(t *testing.T, r rows.Rows, cols []string) {},
		},
		{
			sql: "应该报错_QuerySpec.Select中有非法列",
			before: func(t *testing.T, sql string) ([]rows.Rows, []string) {
				return nil, nil
			},
			originSpec: QuerySpec{
				Select: []merger.ColumnInfo{
					{
						Index: 1,
						Name:  "COUNT(`amount`)",
					},
				},
			},
			targetSpec: QuerySpec{
				Select: []merger.ColumnInfo{
					{
						Index: 1,
						Name:  "COUNT(`amount`)",
					},
				},
			},
			requireErrFunc: func(t require.TestingT, err error, i ...interface{}) {
				require.ErrorIs(t, err, ErrInvalidColumnInfo)
			},
			after: func(t *testing.T, r rows.Rows, cols []string) {},
		},
		{
			sql: "SELECT `id`,`status` FROM `orders`",
			before: func(t *testing.T, sql string) ([]rows.Rows, []string) {
				t.Helper()
				targetSQL := sql
				cols := []string{"`id`", "`status`"}
				s.mock01.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(1, 0).AddRow(3, 1))
				s.mock02.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(2, 1).AddRow(4, 0))
				s.mock03.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols))
				return getResultSet(t, targetSQL, s.db01, s.db02, s.db03), cols
			},
			originSpec: QuerySpec{
				Features: nil,
				Select: []merger.ColumnInfo{
					{
						Index: 0,
						Name:  "id",
					},
					{
						Index: 1,
						Name:  "status",
					},
				},
			},
			targetSpec: QuerySpec{
				Features: nil,
				Select: []merger.ColumnInfo{
					{
						Index: 0,
						Name:  "id",
					},
					{
						Index: 1,
						Name:  "status",
					},
				},
			},
			requireErrFunc: require.NoError,
			after: func(t *testing.T, r rows.Rows, cols []string) {
				t.Helper()

				columnNames, err := r.Columns()
				require.NoError(t, err)
				require.Equal(t, cols, columnNames)

				scanFunc := func(rr rows.Rows, valSet *[]any) error {
					var id, status int
					if err := rr.Scan(&id, &status); err != nil {
						return err
					}
					*valSet = append(*valSet, []any{id, status})
					return nil
				}

				require.Equal(t, []any{
					[]any{1, 0},
					[]any{3, 1},
					[]any{2, 1},
					[]any{4, 0},
				}, getRowValues(t, r, scanFunc))
			},
		},
		// 别名
		{
			sql: "SELECT SUM(`amount`) AS `total_amount`, COUNT(*) AS `cnt_amount` FROM `orders`",
			before: func(t *testing.T, sql string) ([]rows.Rows, []string) {
				t.Helper()
				targetSQL := sql
				cols := []string{"`total_amount`", "`cnt_amount`"}
				s.mock01.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(100, 3))
				s.mock02.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(150, 2))
				s.mock03.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(50, 1))
				return getResultSet(t, targetSQL, s.db01, s.db02, s.db03), cols
			},
			originSpec: QuerySpec{
				Features: []query.Feature{query.AggregateFunc},
				Select: []merger.ColumnInfo{
					{
						Index:         0,
						Name:          "`amount`",
						AggregateFunc: "SUM",
						Alias:         "`total_amount`",
					},
					{
						Index:         1,
						Name:          "*",
						AggregateFunc: "COUNT",
						Alias:         "`cnt_amount`",
					},
				},
			},
			targetSpec: QuerySpec{
				Features: []query.Feature{query.AggregateFunc},
				Select: []merger.ColumnInfo{
					{
						Index:         0,
						Name:          "`amount`",
						AggregateFunc: "SUM",
						Alias:         "`total_amount`",
					},
					{
						Index:         1,
						Name:          "*",
						AggregateFunc: "COUNT",
						Alias:         "`cnt_amount`",
					},
				},
			},
			requireErrFunc: require.NoError,
			after: func(t *testing.T, r rows.Rows, cols []string) {
				t.Helper()

				columnNames, err := r.Columns()
				require.NoError(t, err)
				require.Equal(t, cols, columnNames)

				scanFunc := func(rr rows.Rows, valSet *[]any) error {
					var totalAmt, cnt int
					if err := rr.Scan(&totalAmt, &cnt); err != nil {
						return err
					}
					*valSet = append(*valSet, []any{totalAmt, cnt})
					return nil
				}

				require.Equal(t, []any{
					[]any{300, 6},
				}, getRowValues(t, r, scanFunc))
			},
		},
		// 聚合函数
		{
			sql: "SELECT MIN(`amount`),MAX(`amount`),AVG(`amount`),SUM(`amount`),COUNT(`amount`) FROM `orders` WHERE (`order_id` > 10 AND `amount` > 20) OR `order_id` > 100 OR `amount` > 30",
			before: func(t *testing.T, sql string) ([]rows.Rows, []string) {
				t.Helper()
				targetSQL := "SELECT MIN(`amount`),MAX(`amount`),AVG(`amount`),SUM(`amount`), COUNT(`amount`), SUM(`amount`), COUNT(`amount`) FROM `orders`"
				cols := []string{"MIN(`amount`)", "MAX(`amount`)", "AVG(`amount`)", "SUM(`amount`)", "COUNT(`amount`)", "SUM(`amount`)", "COUNT(`amount`)"}
				s.mock01.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(200, 200, 200, 400, 2, 400, 2))
				s.mock02.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(150, 150, 150, 450, 3, 450, 3))
				s.mock03.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(50, 50, 50, 50, 1, 50, 1))
				return getResultSet(t, targetSQL, s.db01, s.db02, s.db03), cols
			},
			originSpec: QuerySpec{
				Features: []query.Feature{query.AggregateFunc},
				Select: []merger.ColumnInfo{
					{
						Index:         0,
						Name:          "`amount`",
						AggregateFunc: "MIN",
					},
					{
						Index:         1,
						Name:          "`amount`",
						AggregateFunc: "MAX",
					},
					{
						Index:         2,
						Name:          "`amount`",
						AggregateFunc: "AVG",
					},
					{
						Index:         3,
						Name:          "`amount`",
						AggregateFunc: "SUM",
					},
					{
						Index:         4,
						Name:          "`amount`",
						AggregateFunc: "COUNT",
					},
				},
			},
			targetSpec: QuerySpec{
				Features: []query.Feature{query.AggregateFunc},
				Select: []merger.ColumnInfo{
					{
						Index:         0,
						Name:          "`amount`",
						AggregateFunc: "MIN",
					},
					{
						Index:         1,
						Name:          "`amount`",
						AggregateFunc: "MAX",
					},
					{
						Index:         2,
						Name:          "`amount`",
						AggregateFunc: "AVG",
					},
					{
						Index:         3,
						Name:          "`amount`",
						AggregateFunc: "SUM",
					},
					{
						Index:         4,
						Name:          "`amount`",
						AggregateFunc: "COUNT",
					},
					{
						Index:         5,
						Name:          "`amount`",
						AggregateFunc: "SUM",
					},
					{
						Index:         6,
						Name:          "`amount`",
						AggregateFunc: "COUNT",
					},
				},
			},
			requireErrFunc: require.NoError,
			after: func(t *testing.T, r rows.Rows, _ []string) {
				t.Helper()

				cols := []string{"MIN(`amount`)", "MAX(`amount`)", "AVG(`amount`)", "SUM(`amount`)", "COUNT(`amount`)"}
				columnsNames, err := r.Columns()
				require.NoError(t, err)
				require.Equal(t, cols, columnsNames)

				scanFunc := func(rr rows.Rows, valSet *[]any) error {
					var minAmt, maxAmt, sumAmt, cntAmt int
					var avgAmt float64
					if err := rr.Scan(&minAmt, &maxAmt, &avgAmt, &sumAmt, &cntAmt); err != nil {
						return err
					}
					*valSet = append(*valSet, []any{minAmt, maxAmt, avgAmt, sumAmt, cntAmt})
					return nil
				}

				sum := 200*2 + 150*3 + 50
				cnt := 6
				avg := float64(sum) / float64(cnt)
				require.Equal(t, []any{
					[]any{50, 200, avg, sum, cnt},
				}, getRowValues(t, r, scanFunc))
			},
		},
		// ORDER BY
		{
			sql: "应该报错_QuerySpec.OrderBy为空",
			// SELECT `ctime` FROM `orders` ORDER BY `ctime` DESC
			before: func(t *testing.T, sql string) ([]rows.Rows, []string) {
				return nil, nil
			},
			originSpec: QuerySpec{
				Features: []query.Feature{query.OrderBy},
				Select: []merger.ColumnInfo{
					{
						Index: 0,
						Name:  "`ctime`",
					},
				},
				OrderBy: []merger.ColumnInfo{},
			},
			targetSpec: QuerySpec{
				Features: []query.Feature{query.OrderBy},
				Select: []merger.ColumnInfo{
					{
						Index: 0,
						Name:  "`ctime`",
					},
				},
				OrderBy: []merger.ColumnInfo{},
			},
			requireErrFunc: func(t require.TestingT, err error, i ...interface{}) {
				require.ErrorIs(t, err, ErrEmptyColumnList)
			},
			after: func(t *testing.T, r rows.Rows, cols []string) {},
		},
		{
			sql: "应该报错_QuerySpec.OrderBy中的列不在QuerySpec.Select列表中",
			// TODO: ORDER BY中的列不在SELECT列表中
			//       - SELECT * FROM `orders` ORDER BY `ctime` DESC
			//       - SELECT `user_id`, `order_id` FROM `orders` ORDER BY `ctime`;
			before: func(t *testing.T, sql string) ([]rows.Rows, []string) {
				return nil, nil
			},
			originSpec: QuerySpec{
				Features: []query.Feature{query.OrderBy},
				Select: []merger.ColumnInfo{
					{
						Index: 0,
						Name:  "`order_id`",
					},
				},
				OrderBy: []merger.ColumnInfo{
					{
						Index: 0,
						Name:  "`ctime`",
						Order: merger.ASC,
					},
				},
			},
			targetSpec: QuerySpec{
				Features: []query.Feature{query.OrderBy},
				Select: []merger.ColumnInfo{
					{
						Index: 0,
						Name:  "`order_id`",
					},
				},
				OrderBy: []merger.ColumnInfo{
					{
						Index: 0,
						Name:  "`ctime`",
						Order: merger.ASC,
					},
				},
			},
			requireErrFunc: func(t require.TestingT, err error, i ...interface{}) {
				require.ErrorIs(t, err, ErrColumnNotFoundInSelectList)
			},
			after: func(t *testing.T, r rows.Rows, cols []string) {},
		},
		{
			sql: "SELECT `user_id` AS `uid`,`order_id` AS `oid` FROM `orders` ORDER BY `uid`, `oid` DESC",
			before: func(t *testing.T, sql string) ([]rows.Rows, []string) {
				t.Helper()
				targetSQL := sql
				cols := []string{"`uid`", "`oid`"}
				s.mock01.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "oid5").AddRow(1, "oid4").AddRow(3, "oid7").AddRow(3, "oid6"))
				s.mock02.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols))
				s.mock03.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(2, "oid3").AddRow(2, "oid2").AddRow(4, "oid1"))
				return getResultSet(t, targetSQL, s.db01, s.db02, s.db03), cols
			},
			originSpec: QuerySpec{
				Features: []query.Feature{query.OrderBy},
				Select: []merger.ColumnInfo{
					{
						Index: 0,
						Name:  "`user_id`",
						Alias: "`uid`",
					},
					{
						Index: 1,
						Name:  "`order_id`",
						Alias: "`oid`",
					},
				},
				OrderBy: []merger.ColumnInfo{
					{
						Index: 0,
						Name:  "`user_id`",
						Alias: "`uid`",
						Order: merger.ASC,
					},
					{
						Index: 1,
						Name:  "`order_id`",
						Alias: "`oid`",
						Order: merger.DESC,
					},
				},
			},
			targetSpec: QuerySpec{
				Features: []query.Feature{query.OrderBy},
				Select: []merger.ColumnInfo{
					{
						Index: 0,
						Name:  "`user_id`",
						Alias: "`uid`",
					},
					{
						Index: 1,
						Name:  "`order_id`",
						Alias: "`oid`",
					},
				},
				OrderBy: []merger.ColumnInfo{
					{
						Index: 0,
						Name:  "`user_id`",
						Alias: "`uid`",
						Order: merger.ASC,
					},
					{
						Index: 1,
						Name:  "`order_id`",
						Alias: "`oid`",
						Order: merger.DESC,
					},
				},
			},
			requireErrFunc: require.NoError,
			after: func(t *testing.T, r rows.Rows, cols []string) {
				t.Helper()

				columnsNames, err := r.Columns()
				require.NoError(t, err)
				require.Equal(t, cols, columnsNames)

				scanFunc := func(rr rows.Rows, valSet *[]any) error {
					var uid int
					var oid string
					if err := rr.Scan(&uid, &oid); err != nil {
						return err
					}
					*valSet = append(*valSet, []any{uid, oid})
					return nil
				}

				require.Equal(t, []any{
					[]any{1, "oid5"},
					[]any{1, "oid4"},
					[]any{2, "oid3"},
					[]any{2, "oid2"},
					[]any{3, "oid7"},
					[]any{3, "oid6"},
					[]any{4, "oid1"},
				}, getRowValues(t, r, scanFunc))
			},
		},
		// 聚合函数 + ORDER BY
		{
			sql: "SELECT AVG(`amount`) AS `avg_amt` FROM `orders` ORDER BY `avg_amt`",

			before: func(t *testing.T, sql string) ([]rows.Rows, []string) {
				t.Helper()
				targetSQL := "SELECT AVG(`amount`) AS `avg_amt`, SUM(`amount`), COUNT(`amount`) FROM `orders` ORDER BY SUM(`amount`), COUNT(`amount`)"
				cols := []string{"`avg_amt`", "SUM(`amount`)", "COUNT(`amount`)"}
				s.mock01.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(50, 200, 4))
				s.mock02.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(75, 150, 2))
				s.mock03.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(40, 40, 1))
				return getResultSet(t, targetSQL, s.db01, s.db02, s.db03), cols
			},
			originSpec: QuerySpec{
				Features: []query.Feature{query.AggregateFunc, query.OrderBy},
				Select: []merger.ColumnInfo{
					{
						Index:         0,
						Name:          "`amount`",
						AggregateFunc: "AVG",
						Alias:         "`avg_amt`",
					},
				},
				OrderBy: []merger.ColumnInfo{
					{
						Index:         0,
						Name:          "`amount`",
						AggregateFunc: "AVG",
						Alias:         "`avg_amt`",
						Order:         true,
					},
				},
			},
			targetSpec: QuerySpec{
				Features: []query.Feature{query.AggregateFunc, query.OrderBy},
				Select: []merger.ColumnInfo{
					{
						Index:         0,
						Name:          "`amount`",
						AggregateFunc: "AVG",
						Alias:         "`avg_amt`",
					},
					{
						Index:         1,
						Name:          "`amount`",
						AggregateFunc: "SUM",
					},
					{
						Index:         2,
						Name:          "`amount`",
						AggregateFunc: "COUNT",
					},
				},
				OrderBy: []merger.ColumnInfo{
					{
						Index:         0,
						Name:          "`amount`",
						AggregateFunc: "AVG",
						Alias:         "`avg_amt`",
					},
				},
			},
			requireErrFunc: require.NoError,
			after: func(t *testing.T, r rows.Rows, _ []string) {
				t.Helper()
				cols := []string{"`avg_amt`"}
				columnsNames, err := r.Columns()
				require.NoError(t, err)
				require.Equal(t, cols, columnsNames)

				scanFunc := func(rr rows.Rows, valSet *[]any) error {
					var avg float64
					if err := rr.Scan(&avg); err != nil {
						return err
					}
					*valSet = append(*valSet, []any{avg})
					return nil
				}

				avg := float64(200+150+40) / float64(4+2+1)
				require.Equal(t, []any{
					[]any{avg},
				}, getRowValues(t, r, scanFunc))
			},
		},
		{
			// TODO: 暂时用该测试用例替换上方avg案例,当avg问题修复后,该测试用例应该删除
			sql: "SELECT COUNT(`amount`) AS `cnt_amt` FROM `orders` ORDER BY `cnt_amt`",
			before: func(t *testing.T, sql string) ([]rows.Rows, []string) {
				t.Helper()
				targetSQL := sql
				// TODO: 这里如果使用COUNT(`amount`)会报错, 必须使用`cnt_amt`
				cols := []string{"`cnt_amt`"}
				s.mock01.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(4))
				s.mock02.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(2))
				s.mock03.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(1))
				return getResultSet(t, targetSQL, s.db01, s.db02, s.db03), cols
			},
			originSpec: QuerySpec{
				Features: []query.Feature{query.AggregateFunc, query.OrderBy},
				Select: []merger.ColumnInfo{
					{
						Index:         0,
						Name:          "`amount`",
						AggregateFunc: "COUNT",
						Alias:         "`cnt_amt`",
					},
				},
				OrderBy: []merger.ColumnInfo{
					{
						Index:         0,
						Name:          "`amount`",
						AggregateFunc: "COUNT",
						Alias:         "`cnt_amt`",
						Order:         merger.ASC,
					},
				},
			},
			targetSpec: QuerySpec{
				Features: []query.Feature{query.AggregateFunc, query.OrderBy},
				Select: []merger.ColumnInfo{
					{
						Index:         0,
						Name:          "`amount`",
						AggregateFunc: "COUNT",
						Alias:         "`cnt_amt`",
					},
				},
				OrderBy: []merger.ColumnInfo{
					{
						Index:         0,
						Name:          "`amount`",
						AggregateFunc: "COUNT",
						Alias:         "`cnt_amt`",
						Order:         merger.ASC,
					},
				},
			},
			requireErrFunc: require.NoError,
			after: func(t *testing.T, r rows.Rows, cols []string) {
				t.Helper()
				columnsNames, err := r.Columns()
				require.NoError(t, err)
				require.Equal(t, cols, columnsNames)

				scanFunc := func(rr rows.Rows, valSet *[]any) error {
					var cnt int
					if err := rr.Scan(&cnt); err != nil {
						return err
					}
					*valSet = append(*valSet, []any{cnt})
					return nil
				}

				require.Equal(t, []any{
					[]any{4 + 2 + 1},
				}, getRowValues(t, r, scanFunc))
			},
		},
		// GROUP BY
		{
			sql: "应该报错_QuerySpec.GroupBy为空",
			// SELECT `ctime` FROM `orders` ORDER BY `ctime` DESC
			before: func(t *testing.T, sql string) ([]rows.Rows, []string) {
				return nil, nil
			},
			originSpec: QuerySpec{
				Features: []query.Feature{query.GroupBy},
				Select: []merger.ColumnInfo{
					{
						Index: 0,
						Name:  "`ctime`",
					},
				},
			},
			targetSpec: QuerySpec{
				Features: []query.Feature{query.GroupBy},
				Select: []merger.ColumnInfo{
					{
						Index: 0,
						Name:  "`ctime`",
					},
				},
			},
			requireErrFunc: func(t require.TestingT, err error, i ...interface{}) {
				require.ErrorIs(t, err, ErrEmptyColumnList)
			},
			after: func(t *testing.T, r rows.Rows, cols []string) {},
		},
		{
			sql: "应该报错_QuerySpec.GroupBy中的列不在QuerySpec.Select列表中",
			before: func(t *testing.T, sql string) ([]rows.Rows, []string) {
				return nil, nil
			},
			originSpec: QuerySpec{
				Features: []query.Feature{query.GroupBy},
				Select: []merger.ColumnInfo{
					{
						Index: 0,
						Name:  "`order_id`",
					},
				},
				GroupBy: []merger.ColumnInfo{
					{
						Index: 1,
						Name:  "`ctime`",
						Order: merger.ASC,
					},
				},
			},
			targetSpec: QuerySpec{
				Features: []query.Feature{query.GroupBy},
				Select: []merger.ColumnInfo{
					{
						Index: 0,
						Name:  "`order_id`",
					},
				},
				GroupBy: []merger.ColumnInfo{
					{
						Index: 1,
						Name:  "`ctime`",
						Order: merger.ASC,
					},
				},
			},
			requireErrFunc: func(t require.TestingT, err error, i ...interface{}) {
				require.ErrorIs(t, err, ErrColumnNotFoundInSelectList)
			},
			after: func(t *testing.T, r rows.Rows, cols []string) {},
		},
		{
			sql: "应该报错_QuerySpec.Select中非聚合列未出现在QuerySpec.GroupBy列表中",
			before: func(t *testing.T, sql string) ([]rows.Rows, []string) {
				return nil, nil
			},
			originSpec: QuerySpec{
				Features: []query.Feature{query.GroupBy},
				Select: []merger.ColumnInfo{
					{
						Index: 0,
						Name:  "`user_id`",
					},
					{
						Index: 1,
						Name:  "`order_id`",
					},
					{
						Index:         2,
						Name:          "`amount`",
						AggregateFunc: "SUM",
					},
				},
				GroupBy: []merger.ColumnInfo{
					{
						Index: 0,
						Name:  "`user_id`",
					},
				},
			},
			targetSpec: QuerySpec{
				Features: []query.Feature{query.GroupBy},
				Select: []merger.ColumnInfo{
					{
						Index: 0,
						Name:  "`user_id`",
					},
					{
						Index: 1,
						Name:  "`order_id`",
					},
					{
						Index:         1,
						Name:          "`amount`",
						AggregateFunc: "SUM",
					},
				},
				GroupBy: []merger.ColumnInfo{
					{
						Index: 0,
						Name:  "`user_id`",
					},
				},
			},
			requireErrFunc: func(t require.TestingT, err error, i ...interface{}) {
				require.ErrorIs(t, err, ErrInvalidColumnInfo)
			},
			after: func(t *testing.T, r rows.Rows, cols []string) {},
		},
		{
			sql: "应该报错_QuerySpec.Select中的聚合列不能出现在QuerySpec.GroupBy列表中",
			before: func(t *testing.T, sql string) ([]rows.Rows, []string) {
				return nil, nil
			},
			originSpec: QuerySpec{
				Features: []query.Feature{query.GroupBy},
				Select: []merger.ColumnInfo{
					{
						Index:         0,
						Name:          "`amount`",
						AggregateFunc: "SUM",
					},
				},
				GroupBy: []merger.ColumnInfo{
					{
						Index:         0,
						Name:          "`amount`",
						AggregateFunc: "SUM",
					},
				},
			},
			targetSpec: QuerySpec{
				Features: []query.Feature{query.GroupBy},
				Select: []merger.ColumnInfo{
					{
						Index:         0,
						Name:          "`amount`",
						AggregateFunc: "SUM",
					},
				},
				GroupBy: []merger.ColumnInfo{
					{
						Index:         0,
						Name:          "`amount`",
						AggregateFunc: "SUM",
					},
				},
			},
			requireErrFunc: func(t require.TestingT, err error, i ...interface{}) {
				require.ErrorIs(t, err, ErrInvalidColumnInfo)
			},
			after: func(t *testing.T, r rows.Rows, cols []string) {},
		},
		// 分片键 + 别名
		{
			sql: "SELECT `user_id` AS `uid` FROM `orders` GROUP BY `uid`",
			before: func(t *testing.T, sql string) ([]rows.Rows, []string) {
				t.Helper()
				targetSQL := sql
				cols := []string{"`uid`"}
				s.mock01.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(1).AddRow(3))
				s.mock02.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(17))
				s.mock03.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(2).AddRow(4))
				return getResultSet(t, targetSQL, s.db01, s.db02, s.db03), cols
			},
			originSpec: QuerySpec{
				Features: []query.Feature{query.GroupBy},

				Select: []merger.ColumnInfo{
					{
						Index: 0,
						Name:  "`user_id`",
						Alias: "`uid`",
					},
				},
				GroupBy: []merger.ColumnInfo{
					{
						Index: 0,
						Name:  "`user_id`",
						Alias: "`uid`",
					},
				},
			},
			targetSpec: QuerySpec{
				Features: []query.Feature{query.GroupBy},

				Select: []merger.ColumnInfo{
					{
						Index: 0,
						Name:  "`user_id`",
						Alias: "`uid`",
					},
				},
				GroupBy: []merger.ColumnInfo{
					{
						Index: 0,
						Name:  "`user_id`",
						Alias: "`uid`",
					},
				},
			},
			requireErrFunc: require.NoError,
			after: func(t *testing.T, r rows.Rows, cols []string) {
				t.Helper()

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
					[]any{3},
					[]any{17},
					[]any{2},
					[]any{4},
				}, getRowValues(t, r, scanFunc))
			},
		},
		// 非分片键 + 别名
		{
			sql: "SELECT `amount` AS `order_amt` FROM `orders` GROUP BY `order_amt`",
			before: func(t *testing.T, sql string) ([]rows.Rows, []string) {
				t.Helper()
				targetSQL := sql
				cols := []string{"`order_amt`"}
				s.mock01.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(100).AddRow(300))
				s.mock02.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(100))
				s.mock03.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(200).AddRow(400))
				return getResultSet(t, targetSQL, s.db01, s.db02, s.db03), cols
			},
			originSpec: QuerySpec{
				Features: []query.Feature{query.GroupBy},

				Select: []merger.ColumnInfo{
					{
						Index: 0,
						Name:  "`amount`",
						Alias: "`order_amt`",
					},
				},
				GroupBy: []merger.ColumnInfo{
					{
						Index: 0,
						Name:  "`amount`",
						Alias: "`order_amt`",
					},
				},
			},
			targetSpec: QuerySpec{
				Features: []query.Feature{query.GroupBy},

				Select: []merger.ColumnInfo{
					{
						Index: 0,
						Name:  "`amount`",
						Alias: "`order_amt`",
					},
				},
				GroupBy: []merger.ColumnInfo{
					{
						Index: 0,
						Name:  "`amount`",
						Alias: "`order_amt`",
					},
				},
			},
			requireErrFunc: require.NoError,
			after: func(t *testing.T, r rows.Rows, cols []string) {
				t.Helper()

				columnsNames, err := r.Columns()
				require.NoError(t, err)
				require.Equal(t, cols, columnsNames)

				scanFunc := func(rr rows.Rows, valSet *[]any) error {
					var orderAmt int
					if err := rr.Scan(&orderAmt); err != nil {
						return err
					}
					*valSet = append(*valSet, []any{orderAmt})
					return nil
				}

				require.Equal(t, []any{
					[]any{100},
					[]any{300},
					[]any{200},
					[]any{400},
				}, getRowValues(t, r, scanFunc))
			},
		},
		// 非分片键 + 聚合 + 别名
		{
			sql: "SELECT `ctime` AS `date`, SUM(`amount`) FROM `orders` GROUP BY `date`",
			before: func(t *testing.T, sql string) ([]rows.Rows, []string) {
				t.Helper()
				targetSQL := sql
				cols := []string{"`date`", "SUM(`amount`)"}
				s.mock01.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(1000, 350).AddRow(3000, 350))
				s.mock02.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(1000, 250).AddRow(4000, 50))
				s.mock03.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(2000, 100).AddRow(4000, 50))
				return getResultSet(t, targetSQL, s.db01, s.db02, s.db03), cols
			},
			originSpec: QuerySpec{
				Features: []query.Feature{query.GroupBy},
				Select: []merger.ColumnInfo{
					{
						Index: 0,
						Name:  "`ctime`",
						Alias: "`date`",
					},
					{
						Index:         1,
						Name:          "`amount`",
						AggregateFunc: "SUM",
					},
				},
				GroupBy: []merger.ColumnInfo{
					{
						Index: 0,
						Name:  "`ctime`",
						Alias: "`date`",
					},
				},
			},
			targetSpec: QuerySpec{
				Features: []query.Feature{query.GroupBy},
				Select: []merger.ColumnInfo{
					{
						Index: 0,
						Name:  "`ctime`",
						Alias: "`date`",
					},
					{
						Index:         1,
						Name:          "`amount`",
						AggregateFunc: "SUM",
					},
				},
				GroupBy: []merger.ColumnInfo{
					{
						Index: 0,
						Name:  "`ctime`",
						Alias: "`date`",
					},
				},
			},
			requireErrFunc: require.NoError,
			after: func(t *testing.T, r rows.Rows, cols []string) {
				t.Helper()

				columnsNames, err := r.Columns()
				require.NoError(t, err)
				require.Equal(t, cols, columnsNames)

				scanFunc := func(rr rows.Rows, valSet *[]any) error {
					var date int64
					var sumAmt int
					if err := rr.Scan(&date, &sumAmt); err != nil {
						return err
					}
					*valSet = append(*valSet, []any{date, sumAmt})
					return nil
				}

				require.Equal(t, []any{
					[]any{int64(1000), 600},
					[]any{int64(3000), 350},
					[]any{int64(4000), 100},
					[]any{int64(2000), 100},
				}, getRowValues(t, r, scanFunc))
			},
		},
		// 分片键+非分片键+聚合+别名
		{
			sql: "SELECT `user_id` AS `uid`, `ctime` AS `date`, SUM(`amount`) AS `total_amt` FROM `orders` GROUP BY `uid`, `date`",
			before: func(t *testing.T, sql string) ([]rows.Rows, []string) {
				t.Helper()
				targetSQL := sql
				cols := []string{"`uid`", "`date`", "SUM(`amount`)"}
				s.mock01.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(1, 1000, 350).AddRow(1, 3000, 350))
				s.mock02.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(2, 1000, 250).AddRow(4, 4000, 50))
				s.mock03.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(6, 2000, 100).AddRow(9, 4000, 50))
				return getResultSet(t, targetSQL, s.db01, s.db02, s.db03), cols
			},
			originSpec: QuerySpec{
				Features: []query.Feature{query.GroupBy},
				Select: []merger.ColumnInfo{
					{
						Index: 0,
						Name:  "`user_id`",
						Alias: "`uid`",
					},
					{
						Index: 1,
						Name:  "`ctime`",
						Alias: "`date`",
					},
					{
						Index:         2,
						Name:          "`amount`",
						AggregateFunc: "SUM",
					},
				},
				GroupBy: []merger.ColumnInfo{
					{
						Index: 0,
						Name:  "`user_id`",
						Alias: "`uid`",
					},
					{
						Index: 1,
						Name:  "`ctime`",
						Alias: "`date`",
					},
				},
			},
			targetSpec: QuerySpec{
				Features: []query.Feature{query.GroupBy},
				Select: []merger.ColumnInfo{
					{
						Index: 0,
						Name:  "`user_id`",
						Alias: "`uid`",
					},
					{
						Index: 1,
						Name:  "`ctime`",
						Alias: "`date`",
					},
					{
						Index:         2,
						Name:          "`amount`",
						AggregateFunc: "SUM",
					},
				},
				GroupBy: []merger.ColumnInfo{
					{
						Index: 0,
						Name:  "`user_id`",
						Alias: "`uid`",
					},
					{
						Index: 1,
						Name:  "`ctime`",
						Alias: "`date`",
					},
				},
			},
			requireErrFunc: require.NoError,
			after: func(t *testing.T, r rows.Rows, cols []string) {
				t.Helper()

				columnsNames, err := r.Columns()
				require.NoError(t, err)
				require.Equal(t, cols, columnsNames)

				scanFunc := func(rr rows.Rows, valSet *[]any) error {
					var uid int
					var date int64
					var sumAmt int
					if err := rr.Scan(&uid, &date, &sumAmt); err != nil {
						return err
					}
					*valSet = append(*valSet, []any{uid, date, sumAmt})
					return nil
				}

				require.Equal(t, []any{
					[]any{1, int64(1000), 350},
					[]any{1, int64(3000), 350},
					[]any{2, int64(1000), 250},
					[]any{4, int64(4000), 50},
					[]any{6, int64(2000), 100},
					[]any{9, int64(4000), 50},
				}, getRowValues(t, r, scanFunc))
			},
		},
		// GROUP BY 和 ORDER BY 组合
		{
			sql: "SELECT `user_id` AS `uid`, `ctime` AS `date`, SUM(`amount`) AS `total_amt` FROM `orders` GROUP BY `uid`, `date` ORDER BY `total_amt`,`uid` DESC",
			before: func(t *testing.T, sql string) ([]rows.Rows, []string) {
				t.Helper()
				targetSQL := sql
				cols := []string{"`uid`", "`date`", "`total_amt`"}
				s.mock01.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(2, 3000, 350).AddRow(1, 1000, 350))
				s.mock02.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(4, 4000, 50).AddRow(2, 1000, 250))
				s.mock03.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(9, 4000, 50).AddRow(6, 2000, 100))
				return getResultSet(t, targetSQL, s.db01, s.db02, s.db03), cols
			},
			originSpec: QuerySpec{
				Features: []query.Feature{query.GroupBy, query.OrderBy},
				Select: []merger.ColumnInfo{
					{
						Index: 0,
						Name:  "`user_id`",
						Alias: "`uid`",
					},
					{
						Index: 1,
						Name:  "`ctime`",
						Alias: "`date`",
					},
					{
						Index:         2,
						Name:          "`amount`",
						AggregateFunc: "SUM",
						Alias:         "`total_amt`",
					},
				},
				GroupBy: []merger.ColumnInfo{
					{
						Index: 0,
						Name:  "`user_id`",
						Alias: "`uid`",
					},
					{
						Index: 1,
						Name:  "`ctime`",
						Alias: "`date`",
					},
				},
				OrderBy: []merger.ColumnInfo{
					{
						Index:         2,
						Name:          "`amount`",
						AggregateFunc: "SUM",
						Alias:         "`total_amt`",
						Order:         merger.ASC,
					},
					{
						Index: 0,
						Name:  "`user_id`",
						Alias: "`uid`",
						Order: merger.DESC,
					},
				},
			},
			targetSpec: QuerySpec{
				Features: []query.Feature{query.GroupBy, query.OrderBy},
				Select: []merger.ColumnInfo{
					{
						Index: 0,
						Name:  "`user_id`",
						Alias: "`uid`",
					},
					{
						Index: 1,
						Name:  "`ctime`",
						Alias: "`date`",
					},
					{
						Index:         2,
						Name:          "`amount`",
						AggregateFunc: "SUM",
						Alias:         "`total_amt`",
					},
				},
				GroupBy: []merger.ColumnInfo{
					{
						Index: 0,
						Name:  "`user_id`",
						Alias: "`uid`",
					},
					{
						Index: 1,
						Name:  "`ctime`",
						Alias: "`date`",
					},
				},
				OrderBy: []merger.ColumnInfo{
					{
						Index:         2,
						Name:          "`amount`",
						AggregateFunc: "SUM",
						Alias:         "`total_amt`",
						Order:         merger.ASC,
					},
					{
						Index: 0,
						Name:  "`user_id`",
						Alias: "`uid`",
						Order: merger.DESC,
					},
				},
			},
			requireErrFunc: require.NoError,
			after: func(t *testing.T, r rows.Rows, cols []string) {
				t.Helper()

				columnsNames, err := r.Columns()
				require.NoError(t, err)
				require.Equal(t, cols, columnsNames)

				scanFunc := func(rr rows.Rows, valSet *[]any) error {
					var uid int
					var date int64
					var sumAmt int
					if err := rr.Scan(&uid, &date, &sumAmt); err != nil {
						return err
					}
					*valSet = append(*valSet, []any{uid, date, sumAmt})
					return nil
				}

				require.Equal(t, []any{
					[]any{9, int64(4000), 50},
					[]any{4, int64(4000), 50},
					[]any{6, int64(2000), 100},
					[]any{2, int64(1000), 250},
					[]any{2, int64(3000), 350},
					[]any{1, int64(1000), 350},
				}, getRowValues(t, r, scanFunc))
			},
		},
		// LIMIT
		{
			sql: "应该报错_QuerySpec.Limit小于1",
			before: func(t *testing.T, sql string) ([]rows.Rows, []string) {
				return nil, nil
			},
			originSpec: QuerySpec{
				Features: []query.Feature{query.Limit},
				Select: []merger.ColumnInfo{
					{
						Index:         0,
						Name:          "`amount`",
						AggregateFunc: "SUM",
					},
				},
				Limit: 0,
			},
			targetSpec: QuerySpec{
				Features: []query.Feature{query.Limit},
				Select: []merger.ColumnInfo{
					{
						Index:         0,
						Name:          "`amount`",
						AggregateFunc: "SUM",
					},
				},
				Limit: 0,
			},
			requireErrFunc: func(t require.TestingT, err error, i ...interface{}) {
				require.ErrorIs(t, err, ErrInvalidLimit)
			},
			after: func(t *testing.T, r rows.Rows, cols []string) {},
		},
		{
			sql: "应该报错_QuerySpec.Offset不等于0",
			before: func(t *testing.T, sql string) ([]rows.Rows, []string) {
				return nil, nil
			},
			originSpec: QuerySpec{
				Features: []query.Feature{query.Limit},
				Select: []merger.ColumnInfo{
					{
						Index:         0,
						Name:          "`amount`",
						AggregateFunc: "SUM",
					},
				},
				Limit:  1,
				Offset: 3,
			},
			targetSpec: QuerySpec{
				Features: []query.Feature{query.Limit},
				Select: []merger.ColumnInfo{
					{
						Index:         0,
						Name:          "`amount`",
						AggregateFunc: "SUM",
					},
				},
				Limit:  1,
				Offset: 3,
			},
			requireErrFunc: func(t require.TestingT, err error, i ...interface{}) {
				require.ErrorIs(t, err, ErrInvalidOffset)
			},
			after: func(t *testing.T, r rows.Rows, cols []string) {},
		},
		// 组合
		{
			sql: "SELECT `user_id` AS `uid` FROM `orders` Limit 3 OFFSET 0",
			before: func(t *testing.T, sql string) ([]rows.Rows, []string) {
				t.Helper()
				targetSQL := sql
				cols := []string{"`uid`"}
				s.mock01.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(1).AddRow(3))
				s.mock02.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(17))
				s.mock03.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(2).AddRow(4))
				return getResultSet(t, targetSQL, s.db01, s.db02, s.db03), cols
			},
			originSpec: QuerySpec{
				Features: []query.Feature{query.Limit},

				Select: []merger.ColumnInfo{
					{
						Index: 0,
						Name:  "`user_id`",
						Alias: "`uid`",
					},
				},
				Limit: 3,
			},
			targetSpec: QuerySpec{
				Features: []query.Feature{query.Limit},

				Select: []merger.ColumnInfo{
					{
						Index: 0,
						Name:  "`user_id`",
						Alias: "`uid`",
					},
				},
				Limit: 3,
			},
			requireErrFunc: require.NoError,
			after: func(t *testing.T, r rows.Rows, cols []string) {
				t.Helper()

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
					[]any{3},
					[]any{17},
				}, getRowValues(t, r, scanFunc))
			},
		},
		{
			sql: "SELECT `user_id` AS `uid`, SUM(`amount`) AS `total_amt` FROM `orders` GROUP BY `uid` ORDER BY `total_amt` DESC Limit 2 OFFSET 0",
			before: func(t *testing.T, sql string) ([]rows.Rows, []string) {
				t.Helper()
				targetSQL := sql
				cols := []string{"`uid`", "`total_amt`"}
				s.mock01.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(1, 100).AddRow(3, 100))
				s.mock02.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(5, 500).AddRow(3, 200).AddRow(4, 200))
				s.mock03.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(2, 200).AddRow(4, 200))
				return getResultSet(t, targetSQL, s.db01, s.db02, s.db03), cols
			},
			originSpec: QuerySpec{
				Features: []query.Feature{query.GroupBy, query.OrderBy, query.Limit},

				Select: []merger.ColumnInfo{
					{
						Index: 0,
						Name:  "`user_id`",
						Alias: "`uid`",
					},
					{
						Index:         1,
						Name:          "`amount`",
						AggregateFunc: "SUM",
						Alias:         "`total_amt`",
					},
				},
				GroupBy: []merger.ColumnInfo{
					{
						Index: 0,
						Name:  "`user_id`",
						Alias: "`uid`",
					},
				},
				OrderBy: []merger.ColumnInfo{
					{
						Index:         1,
						Name:          "`amount`",
						AggregateFunc: "SUM",
						Alias:         "`total_amt`",
						Order:         merger.DESC,
					},
				},
				Limit: 2,
			},
			targetSpec: QuerySpec{
				Features: []query.Feature{query.GroupBy, query.OrderBy, query.Limit},

				Select: []merger.ColumnInfo{
					{
						Index: 0,
						Name:  "`user_id`",
						Alias: "`uid`",
					},
					{
						Index:         1,
						Name:          "`amount`",
						AggregateFunc: "SUM",
						Alias:         "`total_amt`",
					},
				},
				GroupBy: []merger.ColumnInfo{
					{
						Index: 0,
						Name:  "`user_id`",
						Alias: "`uid`",
					},
				},
				OrderBy: []merger.ColumnInfo{
					{
						Index:         1,
						Name:          "`amount`",
						AggregateFunc: "SUM",
						Alias:         "`total_amt`",
						Order:         merger.DESC,
					},
				},
				Limit: 2,
			},
			requireErrFunc: require.NoError,
			after: func(t *testing.T, r rows.Rows, cols []string) {
				t.Helper()

				columnsNames, err := r.Columns()
				require.NoError(t, err)
				require.Equal(t, cols, columnsNames)

				scanFunc := func(rr rows.Rows, valSet *[]any) error {
					var uid int
					var sumAmt int
					if err := rr.Scan(&uid, &sumAmt); err != nil {
						return err
					}
					*valSet = append(*valSet, []any{uid, sumAmt})
					return nil
				}

				require.Equal(t, []any{
					[]any{5, 500},
					[]any{4, 400},
				}, getRowValues(t, r, scanFunc))
			},
		},
		{
			sql: "SELECT `user_id` AS `uid`, `ctime` AS `date`, SUM(`amount`) AS `total_amt` FROM `orders` GROUP BY `uid`, `date` ORDER BY `total_amt` Limit 6 OFFSET 0",
			before: func(t *testing.T, sql string) ([]rows.Rows, []string) {
				t.Helper()
				targetSQL := sql
				cols := []string{"`uid`", "`date`", "`total_amt`"}
				s.mock01.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(1, 1000, 100).AddRow(3, 3000, 100))
				s.mock02.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(5, 5000, 500).AddRow(3, 3000, 200).AddRow(4, 4000, 200))
				s.mock03.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(2, 2000, 200).AddRow(4, 4001, 200))
				return getResultSet(t, targetSQL, s.db01, s.db02, s.db03), cols
			},
			originSpec: QuerySpec{
				Features: []query.Feature{query.GroupBy, query.OrderBy, query.Limit},

				Select: []merger.ColumnInfo{
					{
						Index: 0,
						Name:  "`user_id`",
						Alias: "`uid`",
					},
					{
						Index: 1,
						Name:  "`ctime`",
						Alias: "`date`",
					},
					{
						Index:         2,
						Name:          "`amount`",
						AggregateFunc: "SUM",
						Alias:         "`total_amt`",
					},
				},
				GroupBy: []merger.ColumnInfo{
					{
						Index: 0,
						Name:  "`user_id`",
						Alias: "`uid`",
					},
					{
						Index: 1,
						Name:  "`ctime`",
						Alias: "`date`",
					},
				},
				OrderBy: []merger.ColumnInfo{
					{
						Index:         2,
						Name:          "`amount`",
						AggregateFunc: "SUM",
						Alias:         "`total_amt`",
						Order:         merger.ASC,
					},
				},
				Limit: 6,
			},
			targetSpec: QuerySpec{
				Features: []query.Feature{query.GroupBy, query.OrderBy, query.Limit},

				Select: []merger.ColumnInfo{
					{
						Index: 0,
						Name:  "`user_id`",
						Alias: "`uid`",
					},
					{
						Index: 1,
						Name:  "`ctime`",
						Alias: "`date`",
					},
					{
						Index:         2,
						Name:          "`amount`",
						AggregateFunc: "SUM",
						Alias:         "`total_amt`",
					},
				},
				GroupBy: []merger.ColumnInfo{
					{
						Index: 0,
						Name:  "`user_id`",
						Alias: "`uid`",
					},
					{
						Index: 1,
						Name:  "`ctime`",
						Alias: "`date`",
					},
				},
				OrderBy: []merger.ColumnInfo{
					{
						Index:         2,
						Name:          "`amount`",
						AggregateFunc: "SUM",
						Alias:         "`total_amt`",
						Order:         merger.ASC,
					},
				},
				Limit: 6,
			},
			requireErrFunc: require.NoError,
			after: func(t *testing.T, r rows.Rows, cols []string) {
				t.Helper()

				columnsNames, err := r.Columns()
				require.NoError(t, err)
				require.Equal(t, cols, columnsNames)

				scanFunc := func(rr rows.Rows, valSet *[]any) error {
					var uid int
					var date int
					var sumAmt int
					if err := rr.Scan(&uid, &date, &sumAmt); err != nil {
						return err
					}
					*valSet = append(*valSet, []any{uid, date, sumAmt})
					return nil
				}

				require.Equal(t, []any{
					[]any{1, 1000, 100},
					[]any{4, 4000, 200},
					[]any{2, 2000, 200},
					[]any{4, 4001, 200},
					[]any{3, 3000, 300},
					[]any{5, 5000, 500},
				}, getRowValues(t, r, scanFunc))
			},
		},
		// 聚合 + 非聚合 + GROUP BY + ORDER BY + LIMIT
		{
			sql: "SELECT `user_id`, COUNT(`amount`) AS `order_count`, AVG(`amount`) FROM `orders` GROUP BY `user_id` ORDER BY `order_count` DESC, `user_id` DESC Limit 4 OFFSET 0",
			before: func(t *testing.T, sql string) ([]rows.Rows, []string) {
				t.Helper()
				targetSQL := "SELECT `user_id`, COUNT(`amount`) AS `order_count`, AVG(`amount`), SUM(`amount`), COUNT(`amount`) FROM `orders` GROUP BY `user_id` ORDER BY `order_count` DESC, `user_id` DESC Limit 3 OFFSET 0"
				cols := []string{"`user_id`", "`order_count`", "AVG(`amount`)", "SUM(`amount`)", "COUNT(`amount`)"}
				s.mock01.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(1, 4, 100, 400, 4).AddRow(3, 2, 150, 300, 2))
				s.mock02.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(4, 1, 200, 200, 1).AddRow(3, 1, 150, 150, 1))
				s.mock03.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(1, 3, 450, 1350, 3).AddRow(5, 1, 50, 50, 1))
				return getResultSet(t, targetSQL, s.db01, s.db02, s.db03), cols
			},
			originSpec: QuerySpec{
				Features: []query.Feature{query.GroupBy, query.OrderBy, query.Limit},
				Select: []merger.ColumnInfo{
					{
						Index: 0,
						Name:  "`user_id`",
					},
					{
						Index:         1,
						Name:          "`amount`",
						AggregateFunc: "COUNT",
						Alias:         "`order_count`",
					},
					{
						Index:         2,
						Name:          "`amount`",
						AggregateFunc: "AVG",
					},
				},
				GroupBy: []merger.ColumnInfo{
					{
						Index: 0,
						Name:  "`user_id`",
					},
				},
				OrderBy: []merger.ColumnInfo{
					{
						Index:         1,
						Name:          "`amount`",
						AggregateFunc: "COUNT",
						Alias:         "`order_count`",
					},
					{
						Index: 0,
						Name:  "`user_id`",
					},
				},
				Limit:  4,
				Offset: 0,
			},
			targetSpec: QuerySpec{
				Features: []query.Feature{query.GroupBy, query.OrderBy, query.Limit},
				Select: []merger.ColumnInfo{
					{
						Index: 0,
						Name:  "`user_id`",
					},
					{
						Index:         1,
						Name:          "`amount`",
						AggregateFunc: "COUNT",
						Alias:         "`order_count`",
					},
					{
						Index:         2,
						Name:          "`amount`",
						AggregateFunc: "AVG",
					},
					{
						Index:         3,
						Name:          "`amount`",
						AggregateFunc: "SUM",
					},
					{
						Index:         4,
						Name:          "`amount`",
						AggregateFunc: "COUNT",
					},
				},
				GroupBy: []merger.ColumnInfo{
					{
						Index: 0,
						Name:  "`user_id`",
					},
				},
				OrderBy: []merger.ColumnInfo{
					{
						Index:         1,
						Name:          "`amount`",
						AggregateFunc: "COUNT",
						Alias:         "`order_count`",
					},
					{
						Index: 0,
						Name:  "`user_id`",
					},
				},
				Limit:  4,
				Offset: 0,
			},
			requireErrFunc: require.NoError,
			after: func(t *testing.T, r rows.Rows, _ []string) {
				t.Helper()
				expectedColumnNames := []string{"`user_id`", "`order_count`", "AVG(`amount`)"}
				columnsNames, err := r.Columns()
				require.NoError(t, err)
				require.Equal(t, expectedColumnNames, columnsNames)

				types, err := r.ColumnTypes()
				require.NoError(t, err)
				typeNames := make([]string, 0, len(types))
				for _, typ := range types {
					typeNames = append(typeNames, typ.Name())
				}
				require.Equal(t, expectedColumnNames, typeNames)

				scanFunc := func(rr rows.Rows, valSet *[]any) error {
					var uid, cnt int
					var avgAmt float64
					if err := rr.Scan(&uid, &cnt, &avgAmt); err != nil {
						return err
					}
					*valSet = append(*valSet, []any{uid, cnt, avgAmt})
					return nil
				}
				require.Equal(t, []any{
					[]any{1, 7, float64(250)},
					[]any{3, 3, float64(150)},
					[]any{5, 1, float64(50)},
					[]any{4, 1, float64(200)},
				}, getRowValues(t, r, scanFunc))
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {

			s.SetupTest()

			resultSet, expectedColumnNames := tt.before(t, tt.sql)
			m, err := New(tt.originSpec, tt.targetSpec)
			tt.requireErrFunc(t, err)

			if err != nil {
				return
			}

			r, err := m.Merge(context.Background(), resultSet)
			require.NoError(t, err)

			tt.after(t, r, expectedColumnNames)

			s.TearDownTest()
		})
	}

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
