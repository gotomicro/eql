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
						ASC:   true,
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
		{
			sql: "SELECT `id`,`status` FROM `orders`",
			before: func(t *testing.T, sql string) ([]rows.Rows, []string) {
				t.Helper()
				targetSQL := sql
				cols := []string{"`id`", "`status`"}
				s.mock01.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(1, 0).AddRow(3, 1))
				s.mock02.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(2, 1).AddRow(4, 0))
				s.mock03.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols))
				return s.getResultSet(t, targetSQL, s.db01, s.db02, s.db03), cols
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
				}, s.getRowValues(t, r, scanFunc))
			},
		},
		{
			sql: "SELECT SUM(`amount`) AS `total_amount`, COUNT(*) AS `cnt_amount` FROM `orders`",
			before: func(t *testing.T, sql string) ([]rows.Rows, []string) {
				t.Helper()
				targetSQL := sql
				cols := []string{"`total_amount`", "`cnt_amount`"}
				s.mock01.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(100, 3))
				s.mock02.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(150, 2))
				s.mock03.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(50, 1))
				return s.getResultSet(t, targetSQL, s.db01, s.db02, s.db03), cols
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
				}, s.getRowValues(t, r, scanFunc))
			},
		},
		{
			sql: "SELECT MIN(`amount`),MAX(`amount`),AVG(`amount`),SUM(`amount`),COUNT(`amount`) FROM `orders` WHERE (`order_id` > 10 AND `amount` > 20) OR `order_id` > 100 OR `amount` > 30",
			before: func(t *testing.T, sql string) ([]rows.Rows, []string) {
				t.Helper()
				targetSQL := "SELECT MIN(`amount`),MAX(`amount`),SUM(`amount`), COUNT(`amount`), SUM(`amount`), COUNT(`amount`) FROM `orders`"
				cols := []string{"MIN(`amount`)", "MAX(`amount`)", "SUM(`amount`)", "COUNT(`amount`)", "SUM(`amount`)", "COUNT(`amount`)"}
				s.mock01.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(200, 200, 400, 2, 400, 2))
				s.mock02.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(150, 150, 450, 3, 450, 3))
				s.mock03.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(50, 50, 50, 1, 50, 1))
				return s.getResultSet(t, targetSQL, s.db01, s.db02, s.db03), cols
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
						AggregateFunc: "SUM",
					},
					{
						Index:         3,
						Name:          "`amount`",
						AggregateFunc: "COUNT",
					},
					{
						Index:         4,
						Name:          "`amount`",
						AggregateFunc: "SUM",
					},
					{
						Index:         5,
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
				avg := float64(sum / cnt)
				require.Equal(t, []any{
					[]any{50, 200, avg, sum, cnt},
				}, s.getRowValues(t, r, scanFunc))
			},
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
				return s.getResultSet(t, targetSQL, s.db01, s.db02, s.db03), cols
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
						ASC:   true,
					},
					{
						Index: 1,
						Name:  "`order_id`",
						Alias: "`oid`",
						ASC:   false,
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
						ASC:   true,
					},
					{
						Index: 1,
						Name:  "`order_id`",
						Alias: "`oid`",
						ASC:   false,
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
				}, s.getRowValues(t, r, scanFunc))
			},
		},
		// order by 和 与聚合列组合
		// order by 列为空,返回error
		// order by 列不在select 列表中 返回错误
		// {
		// 	sql: "SELECT `user_id`, AVG(`amount`), COUNT(*) FROM `orders` GROUP BY `user_id` ORDER BY `ctime` OFFSET 0 Limit 20",
		// 	before: func(t *testing.T, sql string) []rows.Rows {
		// 		t.Helper()
		// 		targetSQL := sql
		// 		cols := []string{"`user_id`", "AVG(`amount`)", "COUNT(*)"}
		// 		s.mock01.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(1, 100, 4).AddRow(3, 150, 2))
		// 		s.mock02.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(4, 200, 1))
		// 		s.mock03.ExpectQuery(targetSQL).WillReturnRows(sqlmock.NewRows(cols).AddRow(2, 450, 3))
		// 		return s.getResultSet(t, targetSQL, s.db01, s.db02, s.db03)
		// 	},
		// 	spec: QuerySpec{
		// 		Features: []query.Feature{query.GroupBy, query.OrderBy, query.Limit},
		// 		Select: []merger.ColumnInfo{
		// 			{
		// 				Index: 0,
		// 				Name:  "`user_id`",
		// 			},
		// 			{
		// 				Index:         1,
		// 				Name:          "AVG(`amount`)",
		// 				AggregateFunc: "AVG",
		// 			},
		// 			{
		// 				Index:         2,
		// 				Name:          "COUNT(*)",
		// 				AggregateFunc: "COUNT",
		// 			},
		// 		},
		// 		GroupBy: []merger.ColumnInfo{
		// 			{
		// 				Index: 0,
		// 				Name:  "user_id",
		// 			},
		// 		},
		// 		OrderBy: []merger.ColumnInfo{
		// 			{
		// 				Index:      1,
		// 				Name:       "COUNT(*)",
		// 				IsASCOrder: true,
		// 			},
		// 		},
		// 		Limit:  2,
		// 		Offset: 0,
		// 	},
		// 	requireErrFunc: require.NoError,
		// 	after: func(t *testing.T, r rows.Rows) {
		// 		t.Helper()
		// 		scanFunc := func(rr rows.Rows, valSet *[]any) error {
		// 			log.Printf("before rr = %#vscan = %#v", rr, *valSet)
		// 			var uid, cnt int
		// 			var avgAmt float64
		// 			if err := rr.Scan(&uid, &avgAmt, &cnt); err != nil {
		// 				return err
		// 			}
		// 			*valSet = append(*valSet, []any{uid, avgAmt, cnt})
		// 			return nil
		// 		}
		// 		// 4, 200, 1
		// 		// 3, 150, 2
		// 		// 2, 450, 3,
		// 		// 1, 100, 4,
		// 		require.Equal(t, []any{
		// 			[]any{4, float64(200), 1},
		// 			[]any{3, float64(150), 2},
		// 		}, s.getRowValues(t, r, scanFunc))
		// 	},
		// },
	}
	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {

			s.SetupTest()

			resultSet, expectedColumnNames := tt.before(t, tt.sql)
			m, err := New(tt.originSpec, tt.targetSpec)
			tt.requireErrFunc(t, err)

			r, err := m.Merge(context.Background(), resultSet)
			require.NoError(t, err)

			tt.after(t, r, expectedColumnNames)

			s.TearDownTest()
		})
	}

}

func (s *factoryTestSuite) getRowValues(t *testing.T, r rows.Rows, scanFunc func(r rows.Rows, valSet *[]any) error) []any {
	var res []any
	for r.Next() {
		require.NoError(t, scanFunc(r, &res))
	}
	return res
}

func (s *factoryTestSuite) getResultSet(t *testing.T, sql string, dbs ...*sql.DB) []rows.Rows {
	resultSet := make([]rows.Rows, 0, len(dbs))
	for _, db := range dbs {
		row, err := db.Query(sql)
		require.NoError(t, err)
		resultSet = append(resultSet, row)
	}
	return resultSet
}
