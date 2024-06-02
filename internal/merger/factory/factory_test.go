package factory

import (
	"testing"

	"github.com/ecodeclub/eorm/internal/merger"
	"github.com/ecodeclub/eorm/internal/merger/internal/aggregatemerger"
	"github.com/ecodeclub/eorm/internal/merger/internal/batchmerger"
	"github.com/ecodeclub/eorm/internal/merger/internal/groupbymerger"
	"github.com/ecodeclub/eorm/internal/merger/internal/pagedmerger"
	"github.com/ecodeclub/eorm/internal/merger/internal/sortmerger"
	"github.com/ecodeclub/eorm/internal/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		spec QuerySpec

		wantMergers    []merger.Merger
		requireErrFunc require.ErrorAssertionFunc
	}{
		// 单一特征的测试用例
		{
			name: "Default",
			sql:  "SELECT name FROM students",
			spec: QuerySpec{
				Select: []merger.ColumnInfo{{Name: "name"}},
			},
			wantMergers: []merger.Merger{
				&batchmerger.Merger{},
			},
			requireErrFunc: require.NoError,
		},
		{
			name: "AggregateFunc",
			sql:  "SELECT COUNT(*) FROM students",
			spec: QuerySpec{
				Features: []query.Feature{query.AggregateFunc},
				Select:   []merger.ColumnInfo{{Name: "COUNT(*)"}},
			},
			wantMergers: []merger.Merger{
				&aggregatemerger.Merger{},
			},
			requireErrFunc: require.NoError,
		},
		{
			name: "GroupBy",
			sql:  "SELECT grade FROM students GROUP BY grade",
			spec: QuerySpec{
				Features: []query.Feature{query.GroupBy},
				GroupBy:  []merger.ColumnInfo{{Name: "grade"}},
			},
			wantMergers: []merger.Merger{
				&groupbymerger.AggregatorMerger{},
			},
			requireErrFunc: require.NoError,
		},
		{
			name: "OrderBy",
			sql:  "SELECT name FROM students ORDER BY grade",
			spec: QuerySpec{
				Features: []query.Feature{query.OrderBy},
				OrderBy:  []merger.ColumnInfo{{Name: "grade"}},
			},
			wantMergers: []merger.Merger{
				&sortmerger.Merger{},
			},
			requireErrFunc: require.NoError,
		},
		{
			name: "Limit",
			sql:  "SELECT name FROM students LIMIT 10",
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
			sql:  "SELECT grade, COUNT(*) FROM students GROUP BY grade",
			spec: QuerySpec{
				Features: []query.Feature{query.AggregateFunc, query.GroupBy},
				Select:   []merger.ColumnInfo{{Name: "grade"}, {Name: "COUNT(*)"}},
				GroupBy:  []merger.ColumnInfo{{Name: "grade"}},
			},
			wantMergers: []merger.Merger{
				&aggregatemerger.Merger{},
				&groupbymerger.AggregatorMerger{},
			},
			requireErrFunc: require.NoError,
		},
		{
			name: "AggregateFunc_OrderBy",
			sql:  "SELECT COUNT(*) FROM students ORDER BY grade",
			spec: QuerySpec{
				Features: []query.Feature{query.AggregateFunc, query.OrderBy},
				Select:   []merger.ColumnInfo{{Name: "COUNT(*)"}},
				OrderBy:  []merger.ColumnInfo{{Name: "grade"}},
			},
			wantMergers: []merger.Merger{
				&aggregatemerger.Merger{},
				&sortmerger.Merger{},
			},
			requireErrFunc: require.NoError,
		},
		{
			name: "AggregateFunc_Limit",
			sql:  "SELECT COUNT(*) FROM students LIMIT 10",
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
			sql:  "SELECT grade FROM students GROUP BY grade ORDER BY grade",
			spec: QuerySpec{
				Features: []query.Feature{query.GroupBy, query.OrderBy},
				GroupBy:  []merger.ColumnInfo{{Name: "grade"}},
				OrderBy:  []merger.ColumnInfo{{Name: "grade"}},
			},
			wantMergers: []merger.Merger{
				&groupbymerger.AggregatorMerger{},
				&sortmerger.Merger{},
			},
			requireErrFunc: require.NoError,
		},
		{
			name: "GroupBy_Limit",
			sql:  "SELECT grade FROM students GROUP BY grade LIMIT 10",
			spec: QuerySpec{
				Features: []query.Feature{query.GroupBy, query.Limit},
				GroupBy:  []merger.ColumnInfo{{Name: "grade"}},
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
			sql:  "SELECT name FROM students ORDER BY grade LIMIT 10",
			spec: QuerySpec{
				Features: []query.Feature{query.OrderBy, query.Limit},
				OrderBy:  []merger.ColumnInfo{{Name: "grade"}},
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
			sql:  "SELECT grade, COUNT(*) FROM students GROUP BY grade ORDER BY COUNT(*)",
			spec: QuerySpec{
				Features: []query.Feature{query.AggregateFunc, query.GroupBy, query.OrderBy},
				Select:   []merger.ColumnInfo{{Name: "grade"}, {Name: "COUNT(*)"}},
				GroupBy:  []merger.ColumnInfo{{Name: "grade"}},
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
			sql:  "SELECT grade, COUNT(*) FROM students GROUP BY grade LIMIT 10",
			spec: QuerySpec{
				Features: []query.Feature{query.AggregateFunc, query.GroupBy, query.Limit},
				Select:   []merger.ColumnInfo{{Name: "grade"}, {Name: "COUNT(*)"}},
				GroupBy:  []merger.ColumnInfo{{Name: "grade"}},
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
			sql:  "SELECT COUNT(*) FROM students ORDER BY grade LIMIT 10",
			spec: QuerySpec{
				Features: []query.Feature{query.AggregateFunc, query.OrderBy, query.Limit},
				Select:   []merger.ColumnInfo{{Name: "COUNT(*)"}},
				OrderBy:  []merger.ColumnInfo{{Name: "grade"}},
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
			sql:  "SELECT grade FROM students GROUP BY grade ORDER BY grade LIMIT 10",
			spec: QuerySpec{
				Features: []query.Feature{query.GroupBy, query.OrderBy, query.Limit},
				GroupBy:  []merger.ColumnInfo{{Name: "grade"}},
				OrderBy:  []merger.ColumnInfo{{Name: "grade"}},
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
			sql:  "SELECT grade, COUNT(*) FROM students GROUP BY grade ORDER BY COUNT(*) LIMIT 10",
			spec: QuerySpec{
				Features: []query.Feature{query.AggregateFunc, query.GroupBy, query.OrderBy, query.Limit},
				Select:   []merger.ColumnInfo{{Name: "grade"}, {Name: "COUNT(*)"}},
				GroupBy:  []merger.ColumnInfo{{Name: "grade"}},
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

			m, err := New(tt.spec)
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
