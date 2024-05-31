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
	"database/sql"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestHp(nodes []*node, columns sortColumns) *Heap {
	h := &Heap{
		sortColumns: columns,
	}
	for _, node := range nodes {
		heap.Push(h, node)
	}
	return h
}

func newTestNodes(sortColsList [][]any) []*node {
	res := make([]*node, 0, len(sortColsList))
	for _, sortCols := range sortColsList {
		n := &node{
			sortCols: sortCols,
		}
		res = append(res, n)
	}
	return res
}

func TestHeap(t *testing.T) {
	testcases := []struct {
		name      string
		nodes     func() []*node
		wantNodes func() []*node
		sortCols  func() sortColumns
	}{
		{
			name: "单个列升序",
			nodes: func() []*node {
				return newTestNodes([][]any{
					{2},
					{5},
					{6},
					{1},
					{0},
				})
			},
			wantNodes: func() []*node {
				return newTestNodes([][]any{
					{0},
					{1},
					{2},
					{5},
					{6},
				})
			},
			sortCols: func() sortColumns {
				sortCols, err := newSortColumns(NewSortColumn("id", ASC))
				require.NoError(t, err)
				return sortCols
			},
		},
		{
			name: "单个列降序",
			nodes: func() []*node {
				return newTestNodes([][]any{
					{2},
					{5},
					{6},
					{1},
					{0},
				})
			},
			wantNodes: func() []*node {
				return newTestNodes([][]any{
					{6},
					{5},
					{2},
					{1},
					{0},
				})
			},
			sortCols: func() sortColumns {
				sortCols, err := newSortColumns(NewSortColumn("id", DESC))
				require.NoError(t, err)
				return sortCols
			},
		},
		{
			name: "三个列顺序：升序,降序,升序",
			nodes: func() []*node {
				return newTestNodes([][]any{
					{2, "b", 1},
					{2, "a", 1},
					{2, "e", 2},
					{2, "e", 1},
					{2, "e", 3},
					{5, "b", 1},
					{5, "a", 1},
					{5, "e", 2},
					{5, "e", 1},
					{5, "e", 3},
					{1, "b", 1},
					{1, "a", 1},
					{1, "e", 2},
					{1, "e", 1},
					{1, "e", 3},
				})
			},
			wantNodes: func() []*node {
				return newTestNodes([][]any{
					{1, "e", 1},
					{1, "e", 2},
					{1, "e", 3},
					{1, "b", 1},
					{1, "a", 1},
					{2, "e", 1},
					{2, "e", 2},
					{2, "e", 3},
					{2, "b", 1},
					{2, "a", 1},
					{5, "e", 1},
					{5, "e", 2},
					{5, "e", 3},
					{5, "b", 1},
					{5, "a", 1},
				})
			},
			sortCols: func() sortColumns {
				sortCols, err := newSortColumns(NewSortColumn("id", ASC), NewSortColumn("name", DESC), NewSortColumn("age", ASC))
				require.NoError(t, err)
				return sortCols
			},
		},
		{
			name: "三个列顺序：降序,升序,降序",
			nodes: func() []*node {
				return newTestNodes([][]any{
					{2, "b", 1},
					{2, "a", 1},
					{2, "e", 2},
					{2, "e", 1},
					{2, "e", 3},
					{5, "b", 1},
					{5, "a", 1},
					{5, "e", 2},
					{5, "e", 1},
					{5, "e", 3},
					{1, "b", 1},
					{1, "a", 1},
					{1, "e", 2},
					{1, "e", 1},
					{1, "e", 3},
				})
			},
			wantNodes: func() []*node {
				return newTestNodes([][]any{
					{5, "a", 1},
					{5, "b", 1},
					{5, "e", 3},
					{5, "e", 2},
					{5, "e", 1},
					{2, "a", 1},
					{2, "b", 1},
					{2, "e", 3},
					{2, "e", 2},
					{2, "e", 1},
					{1, "a", 1},
					{1, "b", 1},
					{1, "e", 3},
					{1, "e", 2},
					{1, "e", 1},
				})
			},
			sortCols: func() sortColumns {
				sortCols, err := newSortColumns(NewSortColumn("id", DESC), NewSortColumn("name", ASC), NewSortColumn("age", DESC))
				require.NoError(t, err)
				return sortCols
			},
		},
		{
			name: "三个列的顺序: 升序,升序,降序",
			nodes: func() []*node {
				return newTestNodes([][]any{
					{2, "b", 1},
					{2, "a", 1},
					{2, "e", 2},
					{2, "e", 1},
					{2, "e", 3},
					{5, "b", 1},
					{5, "a", 1},
					{5, "e", 2},
					{5, "e", 1},
					{5, "e", 3},
					{1, "b", 1},
					{1, "a", 1},
					{1, "e", 2},
					{1, "e", 1},
					{1, "e", 3},
				})
			},
			wantNodes: func() []*node {
				return newTestNodes([][]any{
					{1, "a", 1},
					{1, "b", 1},
					{1, "e", 3},
					{1, "e", 2},
					{1, "e", 1},
					{2, "a", 1},
					{2, "b", 1},
					{2, "e", 3},
					{2, "e", 2},
					{2, "e", 1},
					{5, "a", 1},
					{5, "b", 1},
					{5, "e", 3},
					{5, "e", 2},
					{5, "e", 1},
				})
			},
			sortCols: func() sortColumns {
				sortCols, err := newSortColumns(NewSortColumn("id", ASC), NewSortColumn("name", ASC), NewSortColumn("age", DESC))
				require.NoError(t, err)
				return sortCols
			},
		},
		{
			name: "三个列的顺序: 降序,降序,升序",
			nodes: func() []*node {
				return newTestNodes([][]any{
					{2, "b", 1},
					{2, "a", 1},
					{2, "e", 2},
					{2, "e", 1},
					{2, "e", 3},
					{5, "b", 1},
					{5, "a", 1},
					{5, "e", 2},
					{5, "e", 1},
					{5, "e", 3},
					{1, "b", 1},
					{1, "a", 1},
					{1, "e", 2},
					{1, "e", 1},
					{1, "e", 3},
				})
			},
			wantNodes: func() []*node {
				return newTestNodes([][]any{
					{5, "e", 1},
					{5, "e", 2},
					{5, "e", 3},
					{5, "b", 1},
					{5, "a", 1},
					{2, "e", 1},
					{2, "e", 2},
					{2, "e", 3},
					{2, "b", 1},
					{2, "a", 1},
					{1, "e", 1},
					{1, "e", 2},
					{1, "e", 3},
					{1, "b", 1},
					{1, "a", 1},
				})
			},
			sortCols: func() sortColumns {
				sortCols, err := newSortColumns(NewSortColumn("id", DESC), NewSortColumn("name", DESC), NewSortColumn("age", ASC))
				require.NoError(t, err)
				return sortCols
			},
		},
		{
			name: "三个列的顺序: 降序,降序,降序",
			nodes: func() []*node {
				return newTestNodes([][]any{
					{2, "b", 1},
					{2, "a", 1},
					{2, "e", 2},
					{2, "e", 1},
					{2, "e", 3},
					{5, "b", 1},
					{5, "a", 1},
					{5, "e", 2},
					{5, "e", 1},
					{5, "e", 3},
					{1, "b", 1},
					{1, "a", 1},
					{1, "e", 2},
					{1, "e", 1},
					{1, "e", 3},
				})
			},
			wantNodes: func() []*node {
				return newTestNodes([][]any{
					{5, "e", 3},
					{5, "e", 2},
					{5, "e", 1},
					{5, "b", 1},
					{5, "a", 1},
					{2, "e", 3},
					{2, "e", 2},
					{2, "e", 1},
					{2, "b", 1},
					{2, "a", 1},
					{1, "e", 3},
					{1, "e", 2},
					{1, "e", 1},
					{1, "b", 1},
					{1, "a", 1},
				})
			},
			sortCols: func() sortColumns {
				sortCols, err := newSortColumns(NewSortColumn("id", DESC), NewSortColumn("name", DESC), NewSortColumn("age", DESC))
				require.NoError(t, err)
				return sortCols
			},
		},
		{
			name: "三个列的顺序: 升序,升序,升序",
			nodes: func() []*node {
				return newTestNodes([][]any{
					{2, "b", 1},
					{2, "a", 1},
					{2, "e", 2},
					{2, "e", 1},
					{2, "e", 3},
					{5, "b", 1},
					{5, "a", 1},
					{5, "e", 2},
					{5, "e", 1},
					{5, "e", 3},
					{1, "b", 1},
					{1, "a", 1},
					{1, "e", 2},
					{1, "e", 1},
					{1, "e", 3},
				})
			},
			wantNodes: func() []*node {
				return newTestNodes([][]any{
					{1, "a", 1},
					{1, "b", 1},
					{1, "e", 1},
					{1, "e", 2},
					{1, "e", 3},
					{2, "a", 1},
					{2, "b", 1},
					{2, "e", 1},
					{2, "e", 2},
					{2, "e", 3},
					{5, "a", 1},
					{5, "b", 1},
					{5, "e", 1},
					{5, "e", 2},
					{5, "e", 3},
				})
			},
			sortCols: func() sortColumns {
				sortCols, err := newSortColumns(NewSortColumn("id", ASC), NewSortColumn("name", ASC), NewSortColumn("age", ASC))
				require.NoError(t, err)
				return sortCols
			},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			h := newTestHp(tc.nodes(), tc.sortCols())
			res := make([]*node, 0, h.Len())
			for h.Len() > 0 {
				res = append(res, heap.Pop(h).(*node))
			}
			assert.Equal(t, tc.wantNodes(), res)
		})
	}

}

func TestHeap_Nullable(t *testing.T) {
	testcases := []struct {
		name      string
		nodes     func() []*node
		wantNodes func() []*node
		sortCols  func() sortColumns
	}{
		{
			name: "sql.NullInt64 asc",
			nodes: func() []*node {
				return newTestNodes([][]any{
					{sql.NullInt64{Int64: 5, Valid: true}},
					{sql.NullInt64{Int64: 1, Valid: true}},
					{sql.NullInt64{Int64: 3, Valid: true}},
					{sql.NullInt64{Int64: 2, Valid: true}},
					{sql.NullInt64{Int64: 10, Valid: false}},
				})
			},
			wantNodes: func() []*node {
				return newTestNodes([][]any{
					{sql.NullInt64{Int64: 10, Valid: false}},
					{sql.NullInt64{Int64: 1, Valid: true}},
					{sql.NullInt64{Int64: 2, Valid: true}},
					{sql.NullInt64{Int64: 3, Valid: true}},
					{sql.NullInt64{Int64: 5, Valid: true}},
				})
			},
			sortCols: func() sortColumns {
				sortCols, err := newSortColumns(NewSortColumn("id", ASC))
				require.NoError(t, err)
				return sortCols
			},
		},
		{
			name: "sql.NullInt64 desc",
			nodes: func() []*node {
				return newTestNodes([][]any{
					{sql.NullInt64{Int64: 5, Valid: true}},
					{sql.NullInt64{Int64: 1, Valid: true}},
					{sql.NullInt64{Int64: 3, Valid: true}},
					{sql.NullInt64{Int64: 2, Valid: true}},
					{sql.NullInt64{Int64: 10, Valid: false}},
				})
			},
			wantNodes: func() []*node {
				return newTestNodes([][]any{
					{sql.NullInt64{Int64: 5, Valid: true}},
					{sql.NullInt64{Int64: 3, Valid: true}},
					{sql.NullInt64{Int64: 2, Valid: true}},
					{sql.NullInt64{Int64: 1, Valid: true}},
					{sql.NullInt64{Int64: 10, Valid: false}},
				})
			},
			sortCols: func() sortColumns {
				sortCols, err := newSortColumns(NewSortColumn("id", DESC))
				require.NoError(t, err)
				return sortCols
			},
		},
		{
			name: "sql.NullString asc",
			nodes: func() []*node {
				return newTestNodes([][]any{
					{sql.NullString{String: "ab", Valid: true}},
					{sql.NullString{String: "cd", Valid: true}},
					{sql.NullString{String: "bc", Valid: true}},
					{sql.NullString{String: "ba", Valid: true}},
					{sql.NullString{String: "z", Valid: false}},
				})
			},
			wantNodes: func() []*node {
				return newTestNodes([][]any{
					{sql.NullString{String: "z", Valid: false}},
					{sql.NullString{String: "ab", Valid: true}},
					{sql.NullString{String: "ba", Valid: true}},
					{sql.NullString{String: "bc", Valid: true}},
					{sql.NullString{String: "cd", Valid: true}},
				})
			},
			sortCols: func() sortColumns {
				sortCols, err := newSortColumns(NewSortColumn("name", ASC))
				require.NoError(t, err)
				return sortCols
			},
		},
		{
			name: "sql.NullString desc",
			nodes: func() []*node {
				return newTestNodes([][]any{
					{sql.NullString{String: "ab", Valid: true}},
					{sql.NullString{String: "cd", Valid: true}},
					{sql.NullString{String: "bc", Valid: true}},
					{sql.NullString{String: "z", Valid: false}},
					{sql.NullString{String: "ba", Valid: true}},
				})
			},
			wantNodes: func() []*node {
				return newTestNodes([][]any{
					{sql.NullString{String: "cd", Valid: true}},
					{sql.NullString{String: "bc", Valid: true}},
					{sql.NullString{String: "ba", Valid: true}},
					{sql.NullString{String: "ab", Valid: true}},
					{sql.NullString{String: "z", Valid: false}},
				})
			},
			sortCols: func() sortColumns {
				sortCols, err := newSortColumns(NewSortColumn("name", DESC))
				require.NoError(t, err)
				return sortCols
			},
		},
		{
			name: "sql.NullInt16 asc",
			nodes: func() []*node {
				return newTestNodes([][]any{
					{sql.NullInt16{Int16: 5, Valid: true}},
					{sql.NullInt16{Int16: 1, Valid: true}},
					{sql.NullInt16{Int16: 3, Valid: true}},
					{sql.NullInt16{Int16: 2, Valid: true}},
					{sql.NullInt16{Int16: 10, Valid: false}},
				})
			},
			wantNodes: func() []*node {
				return newTestNodes([][]any{
					{sql.NullInt16{Int16: 10, Valid: false}},
					{sql.NullInt16{Int16: 1, Valid: true}},
					{sql.NullInt16{Int16: 2, Valid: true}},
					{sql.NullInt16{Int16: 3, Valid: true}},
					{sql.NullInt16{Int16: 5, Valid: true}},
				})
			},
			sortCols: func() sortColumns {
				sortCols, err := newSortColumns(NewSortColumn("id", ASC))
				require.NoError(t, err)
				return sortCols
			},
		},
		{
			name: "sql.NullInt16 desc",
			nodes: func() []*node {
				return newTestNodes([][]any{
					{sql.NullInt16{Int16: 5, Valid: true}},
					{sql.NullInt16{Int16: 1, Valid: true}},
					{sql.NullInt16{Int16: 3, Valid: true}},
					{sql.NullInt16{Int16: 2, Valid: true}},
					{sql.NullInt16{Int16: 10, Valid: false}},
				})
			},
			wantNodes: func() []*node {
				return newTestNodes([][]any{
					{sql.NullInt16{Int16: 5, Valid: true}},
					{sql.NullInt16{Int16: 3, Valid: true}},
					{sql.NullInt16{Int16: 2, Valid: true}},
					{sql.NullInt16{Int16: 1, Valid: true}},
					{sql.NullInt16{Int16: 10, Valid: false}},
				})
			},
			sortCols: func() sortColumns {
				sortCols, err := newSortColumns(NewSortColumn("id", DESC))
				require.NoError(t, err)
				return sortCols
			},
		},
		{
			name: "sql.NullInt32 asc",
			nodes: func() []*node {
				return newTestNodes([][]any{
					{sql.NullInt32{Int32: 5, Valid: true}},
					{sql.NullInt32{Int32: 1, Valid: true}},
					{sql.NullInt32{Int32: 3, Valid: true}},
					{sql.NullInt32{Int32: 2, Valid: true}},
					{sql.NullInt32{Int32: 10, Valid: false}},
				})
			},
			wantNodes: func() []*node {
				return newTestNodes([][]any{
					{sql.NullInt32{Int32: 10, Valid: false}},
					{sql.NullInt32{Int32: 1, Valid: true}},
					{sql.NullInt32{Int32: 2, Valid: true}},
					{sql.NullInt32{Int32: 3, Valid: true}},
					{sql.NullInt32{Int32: 5, Valid: true}},
				})
			},
			sortCols: func() sortColumns {
				sortCols, err := newSortColumns(NewSortColumn("id", ASC))
				require.NoError(t, err)
				return sortCols
			},
		},
		{
			name: "sql.NullInt32 desc",
			nodes: func() []*node {
				return newTestNodes([][]any{
					{sql.NullInt32{Int32: 5, Valid: true}},
					{sql.NullInt32{Int32: 1, Valid: true}},
					{sql.NullInt32{Int32: 3, Valid: true}},
					{sql.NullInt32{Int32: 2, Valid: true}},
					{sql.NullInt32{Int32: 10, Valid: false}},
				})
			},
			wantNodes: func() []*node {
				return newTestNodes([][]any{
					{sql.NullInt32{Int32: 5, Valid: true}},
					{sql.NullInt32{Int32: 3, Valid: true}},
					{sql.NullInt32{Int32: 2, Valid: true}},
					{sql.NullInt32{Int32: 1, Valid: true}},
					{sql.NullInt32{Int32: 10, Valid: false}},
				})
			},
			sortCols: func() sortColumns {
				sortCols, err := newSortColumns(NewSortColumn("id", DESC))
				require.NoError(t, err)
				return sortCols
			},
		},
		{
			name: "sql.NullFloat64 asc",
			nodes: func() []*node {
				return newTestNodes([][]any{
					{sql.NullFloat64{Float64: 5.0, Valid: true}},
					{sql.NullFloat64{Float64: 1.0, Valid: true}},
					{sql.NullFloat64{Float64: 3.0, Valid: true}},
					{sql.NullFloat64{Float64: 2.0, Valid: true}},
					{sql.NullFloat64{Float64: 10.0, Valid: false}},
				})
			},
			wantNodes: func() []*node {
				return newTestNodes([][]any{
					{sql.NullFloat64{Float64: 10.0, Valid: false}},
					{sql.NullFloat64{Float64: 1.0, Valid: true}},
					{sql.NullFloat64{Float64: 2.0, Valid: true}},
					{sql.NullFloat64{Float64: 3.0, Valid: true}},
					{sql.NullFloat64{Float64: 5.0, Valid: true}},
				})
			},
			sortCols: func() sortColumns {
				sortCols, err := newSortColumns(NewSortColumn("id", ASC))
				require.NoError(t, err)
				return sortCols
			},
		},
		{
			name: "sql.NullFloat64 desc",
			nodes: func() []*node {
				return newTestNodes([][]any{
					{sql.NullFloat64{Float64: 5.0, Valid: true}},
					{sql.NullFloat64{Float64: 1.0, Valid: true}},
					{sql.NullFloat64{Float64: 3.0, Valid: true}},
					{sql.NullFloat64{Float64: 2.0, Valid: true}},
					{sql.NullFloat64{Float64: 10.0, Valid: false}},
				})
			},
			wantNodes: func() []*node {
				return newTestNodes([][]any{
					{sql.NullFloat64{Float64: 5.0, Valid: true}},
					{sql.NullFloat64{Float64: 3.0, Valid: true}},
					{sql.NullFloat64{Float64: 2.0, Valid: true}},
					{sql.NullFloat64{Float64: 1.0, Valid: true}},
					{sql.NullFloat64{Float64: 10.0, Valid: false}},
				})
			},
			sortCols: func() sortColumns {
				sortCols, err := newSortColumns(NewSortColumn("id", DESC))
				require.NoError(t, err)
				return sortCols
			},
		},

		{
			name: "sql.NullTime asc",
			nodes: func() []*node {
				return newTestNodes([][]any{
					{sql.NullTime{Time: func() time.Time {
						time, err := time.ParseInLocation("2006-01-02 15:04:05", "2022-01-01 12:00:00", time.Local)
						require.NoError(t, err)
						return time
					}(), Valid: true}},
					{sql.NullTime{Time: func() time.Time {
						time, err := time.ParseInLocation("2006-01-02 15:04:05", "2022-01-02 12:00:00", time.Local)
						require.NoError(t, err)
						return time
					}(), Valid: true}},
					{sql.NullTime{Time: func() time.Time {
						time, err := time.ParseInLocation("2006-01-02 15:04:05", "2022-01-09 12:00:00", time.Local)
						require.NoError(t, err)
						return time
					}(), Valid: true}},
					{sql.NullTime{Time: func() time.Time {
						time, err := time.ParseInLocation("2006-01-02 15:04:05", "2022-01-01 11:00:00", time.Local)
						require.NoError(t, err)
						return time
					}(), Valid: true}},
					{sql.NullTime{Time: func() time.Time {
						time, err := time.ParseInLocation("2006-01-02 15:04:05", "2022-01-20 12:00:00", time.Local)
						require.NoError(t, err)
						return time
					}(), Valid: false}},
				})
			},
			wantNodes: func() []*node {
				return newTestNodes([][]any{
					{sql.NullTime{Time: func() time.Time {
						time, err := time.ParseInLocation("2006-01-02 15:04:05", "2022-01-20 12:00:00", time.Local)
						require.NoError(t, err)
						return time
					}(), Valid: false}},
					{sql.NullTime{Time: func() time.Time {
						time, err := time.ParseInLocation("2006-01-02 15:04:05", "2022-01-01 11:00:00", time.Local)
						require.NoError(t, err)
						return time
					}(), Valid: true}},
					{sql.NullTime{Time: func() time.Time {
						time, err := time.ParseInLocation("2006-01-02 15:04:05", "2022-01-01 12:00:00", time.Local)
						require.NoError(t, err)
						return time
					}(), Valid: true}},
					{sql.NullTime{Time: func() time.Time {
						time, err := time.ParseInLocation("2006-01-02 15:04:05", "2022-01-02 12:00:00", time.Local)
						require.NoError(t, err)
						return time
					}(), Valid: true}},
					{sql.NullTime{Time: func() time.Time {
						time, err := time.ParseInLocation("2006-01-02 15:04:05", "2022-01-09 12:00:00", time.Local)
						require.NoError(t, err)
						return time
					}(), Valid: true}},
				})
			},
			sortCols: func() sortColumns {
				sortCols, err := newSortColumns(NewSortColumn("time", ASC))
				require.NoError(t, err)
				return sortCols
			},
		},
		{
			name: "sql.NullTime desc",
			nodes: func() []*node {
				return newTestNodes([][]any{
					{sql.NullTime{Time: func() time.Time {
						time, err := time.ParseInLocation("2006-01-02 15:04:05", "2022-01-01 12:00:00", time.Local)
						require.NoError(t, err)
						return time
					}(), Valid: true}},
					{sql.NullTime{Time: func() time.Time {
						time, err := time.ParseInLocation("2006-01-02 15:04:05", "2022-01-02 12:00:00", time.Local)
						require.NoError(t, err)
						return time
					}(), Valid: true}},
					{sql.NullTime{Time: func() time.Time {
						time, err := time.ParseInLocation("2006-01-02 15:04:05", "2022-01-09 12:00:00", time.Local)
						require.NoError(t, err)
						return time
					}(), Valid: true}},
					{sql.NullTime{Time: func() time.Time {
						time, err := time.ParseInLocation("2006-01-02 15:04:05", "2022-01-01 11:00:00", time.Local)
						require.NoError(t, err)
						return time
					}(), Valid: true}},
					{sql.NullTime{Time: func() time.Time {
						time, err := time.ParseInLocation("2006-01-02 15:04:05", "2022-01-20 12:00:00", time.Local)
						require.NoError(t, err)
						return time
					}(), Valid: false}},
				})
			},
			wantNodes: func() []*node {
				return newTestNodes([][]any{
					{sql.NullTime{Time: func() time.Time {
						time, err := time.ParseInLocation("2006-01-02 15:04:05", "2022-01-09 12:00:00", time.Local)
						require.NoError(t, err)
						return time
					}(), Valid: true}},
					{sql.NullTime{Time: func() time.Time {
						time, err := time.ParseInLocation("2006-01-02 15:04:05", "2022-01-02 12:00:00", time.Local)
						require.NoError(t, err)
						return time
					}(), Valid: true}},
					{sql.NullTime{Time: func() time.Time {
						time, err := time.ParseInLocation("2006-01-02 15:04:05", "2022-01-01 12:00:00", time.Local)
						require.NoError(t, err)
						return time
					}(), Valid: true}},
					{sql.NullTime{Time: func() time.Time {
						time, err := time.ParseInLocation("2006-01-02 15:04:05", "2022-01-01 11:00:00", time.Local)
						require.NoError(t, err)
						return time
					}(), Valid: true}},
					{sql.NullTime{Time: func() time.Time {
						time, err := time.ParseInLocation("2006-01-02 15:04:05", "2022-01-20 12:00:00", time.Local)
						require.NoError(t, err)
						return time
					}(), Valid: false}},
				})
			},
			sortCols: func() sortColumns {
				sortCols, err := newSortColumns(NewSortColumn("time", DESC))
				require.NoError(t, err)
				return sortCols
			},
		},
		{
			name: "sql.NullByte asc",
			nodes: func() []*node {
				return newTestNodes([][]any{
					{sql.NullByte{Byte: 'a', Valid: true}},
					{sql.NullByte{Byte: 'c', Valid: true}},
					{sql.NullByte{Byte: 'b', Valid: true}},
					{sql.NullByte{Byte: 'k', Valid: true}},
					{sql.NullByte{Byte: 'z', Valid: false}},
				})
			},
			wantNodes: func() []*node {
				return newTestNodes([][]any{
					{sql.NullByte{Byte: 'z', Valid: false}},
					{sql.NullByte{Byte: 'a', Valid: true}},
					{sql.NullByte{Byte: 'b', Valid: true}},
					{sql.NullByte{Byte: 'c', Valid: true}},
					{sql.NullByte{Byte: 'k', Valid: true}},
				})
			},
			sortCols: func() sortColumns {
				sortCols, err := newSortColumns(NewSortColumn("byte", ASC))
				require.NoError(t, err)
				return sortCols
			},
		},
		{
			name: "sql.NullByte desc",
			nodes: func() []*node {
				return newTestNodes([][]any{
					{sql.NullByte{Byte: 'a', Valid: true}},
					{sql.NullByte{Byte: 'c', Valid: true}},
					{sql.NullByte{Byte: 'b', Valid: true}},
					{sql.NullByte{Byte: 'k', Valid: true}},
					{sql.NullByte{Byte: 'z', Valid: false}},
				})
			},
			wantNodes: func() []*node {
				return newTestNodes([][]any{
					{sql.NullByte{Byte: 'k', Valid: true}},
					{sql.NullByte{Byte: 'c', Valid: true}},
					{sql.NullByte{Byte: 'b', Valid: true}},
					{sql.NullByte{Byte: 'a', Valid: true}},
					{sql.NullByte{Byte: 'z', Valid: false}},
				})
			},
			sortCols: func() sortColumns {
				sortCols, err := newSortColumns(NewSortColumn("byte", DESC))
				require.NoError(t, err)
				return sortCols
			},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			h := newTestHp(tc.nodes(), tc.sortCols())
			res := make([]*node, 0, h.Len())
			for h.Len() > 0 {
				res = append(res, heap.Pop(h).(*node))
			}
			assert.Equal(t, tc.wantNodes(), res)
		})
	}
}

func (ms *MergerSuite) TestCompare() {
	testcases := []struct {
		name    string
		values  []any
		order   Order
		wantVal int
		kind    reflect.Kind
	}{
		{
			name:    "int8 ASC 1,2",
			values:  []any{int8(1), int8(2)},
			order:   ASC,
			wantVal: -1,
			kind:    reflect.Int8,
		},
		{
			name:    "int8 DESC 1,2",
			values:  []any{int8(1), int8(2)},
			order:   DESC,
			wantVal: 1,
			kind:    reflect.Int8,
		},
		{
			name:    "int8 ASC 2,1",
			values:  []any{int8(2), int8(1)},
			order:   ASC,
			wantVal: 1,
			kind:    reflect.Int8,
		},
		{
			name:    "int8 DESC 2,1",
			values:  []any{int8(2), int8(1)},
			order:   DESC,
			wantVal: -1,
			kind:    reflect.Int8,
		},
		{
			name:    "int8 equal",
			values:  []any{int8(2), int8(2)},
			order:   DESC,
			wantVal: 0,
			kind:    reflect.Int8,
		},
		{
			name:    "int16 ASC 1,2",
			values:  []any{int16(1), int16(2)},
			order:   ASC,
			wantVal: -1,
			kind:    reflect.Int16,
		},
		{
			name:    "int16 DESC 1,2",
			values:  []any{int16(1), int16(2)},
			order:   DESC,
			wantVal: 1,
			kind:    reflect.Int16,
		},
		{
			name:    "int16 ASC 2,1",
			values:  []any{int16(2), int16(1)},
			order:   ASC,
			wantVal: 1,
			kind:    reflect.Int16,
		},
		{
			name:    "int16 DESC 2,1",
			values:  []any{int16(2), int16(1)},
			order:   DESC,
			wantVal: -1,
			kind:    reflect.Int16,
		},
		{
			name:    "int16 equa",
			values:  []any{int16(2), int16(2)},
			order:   DESC,
			wantVal: 0,
			kind:    reflect.Int16,
		},
		{
			name:    "int32 ASC 1,2",
			values:  []any{int32(1), int32(2)},
			order:   ASC,
			wantVal: -1,
			kind:    reflect.Int32,
		},
		{
			name:    "int32 DESC 1,2",
			values:  []any{int32(1), int32(2)},
			order:   DESC,
			wantVal: 1,
			kind:    reflect.Int32,
		},
		{
			name:    "int32 ASC 2,1",
			values:  []any{int32(2), int32(1)},
			order:   ASC,
			wantVal: 1,
			kind:    reflect.Int32,
		},
		{
			name:    "int32 DESC 2,1",
			values:  []any{int32(2), int32(1)},
			order:   DESC,
			wantVal: -1,
			kind:    reflect.Int32,
		},
		{
			name:    "int32 equal",
			values:  []any{int32(2), int32(2)},
			order:   DESC,
			wantVal: 0,
			kind:    reflect.Int32,
		},
		{
			name:    "int64 ASC 1,2",
			values:  []any{int64(1), int64(02)},
			order:   ASC,
			wantVal: -1,
			kind:    reflect.Int64,
		},
		{
			name:    "int64 DESC 1,2",
			values:  []any{int64(1), int64(2)},
			order:   DESC,
			wantVal: 1,
			kind:    reflect.Int64,
		},
		{
			name:    "int64 ASC 2,1",
			values:  []any{int64(2), int64(1)},
			order:   ASC,
			wantVal: 1,
			kind:    reflect.Int64,
		},
		{
			name:    "int64 DESC 2,1",
			values:  []any{int64(2), int64(1)},
			order:   DESC,
			wantVal: -1,
			kind:    reflect.Int64,
		},
		{
			name:    "int64 equal",
			values:  []any{int64(2), int64(2)},
			order:   DESC,
			wantVal: 0,
			kind:    reflect.Int64,
		},
		{
			name:    "uint8 ASC 1,2",
			values:  []any{uint8(1), uint8(2)},
			order:   ASC,
			wantVal: -1,
			kind:    reflect.Uint8,
		},
		{
			name:    "uint8 DESC 1,2",
			values:  []any{uint8(1), uint8(2)},
			order:   DESC,
			wantVal: 1,
			kind:    reflect.Uint8,
		},
		{
			name:    "uint8 ASC 2,1",
			values:  []any{uint8(2), uint8(1)},
			order:   ASC,
			wantVal: 1,
			kind:    reflect.Uint8,
		},
		{
			name:    "uint8 DESC 2,1",
			values:  []any{uint8(2), uint8(1)},
			order:   DESC,
			wantVal: -1,
			kind:    reflect.Uint8,
		},
		{
			name:    "uint8 equal",
			values:  []any{uint8(2), uint8(2)},
			order:   DESC,
			wantVal: 0,
			kind:    reflect.Uint8,
		},

		{
			name:    "uint16 ASC 1,2",
			values:  []any{uint16(1), uint16(2)},
			order:   ASC,
			wantVal: -1,
			kind:    reflect.Uint16,
		},
		{
			name:    "uint16 DESC 1,2",
			values:  []any{uint16(1), uint16(2)},
			order:   DESC,
			wantVal: 1,
			kind:    reflect.Uint16,
		},
		{
			name:    "uint16 ASC 2,1",
			values:  []any{uint16(2), uint16(1)},
			order:   ASC,
			wantVal: 1,
			kind:    reflect.Uint16,
		},
		{
			name:    "uint16 DESC 2,1",
			values:  []any{uint16(2), uint16(1)},
			order:   DESC,
			wantVal: -1,
			kind:    reflect.Uint16,
		},
		{
			name:    "uint16 equal",
			values:  []any{uint16(2), uint16(2)},
			order:   DESC,
			wantVal: 0,
			kind:    reflect.Uint16,
		},
		{
			name:    "uint32 ASC 1,2",
			values:  []any{uint32(1), uint32(2)},
			order:   ASC,
			wantVal: -1,
			kind:    reflect.Uint32,
		},
		{
			name:    "uint32 DESC 1,2",
			values:  []any{uint32(1), uint32(2)},
			order:   DESC,
			wantVal: 1,
			kind:    reflect.Uint32,
		},
		{
			name:    "uint32 ASC 2,1",
			values:  []any{uint32(2), uint32(1)},
			order:   ASC,
			wantVal: 1,
			kind:    reflect.Uint32,
		},
		{
			name:    "uint32 DESC 2,1",
			values:  []any{uint32(2), uint32(1)},
			order:   DESC,
			wantVal: -1,
			kind:    reflect.Uint32,
		},
		{
			name:    "uint32 equal",
			values:  []any{uint32(2), uint32(2)},
			order:   DESC,
			wantVal: 0,
			kind:    reflect.Uint32,
		},
		{
			name:    "uint64 ASC 1,2",
			values:  []any{uint64(1), uint64(2)},
			order:   ASC,
			wantVal: -1,
			kind:    reflect.Uint64,
		},
		{
			name:    "uint64 DESC 1,2",
			values:  []any{uint64(1), uint64(2)},
			order:   DESC,
			wantVal: 1,
			kind:    reflect.Uint64,
		},
		{
			name:    "uint64 ASC 2,1",
			values:  []any{uint64(2), uint64(1)},
			order:   ASC,
			wantVal: 1,
			kind:    reflect.Uint64,
		},
		{
			name:    "uint64 DESC 2,1",
			values:  []any{uint64(2), uint64(1)},
			order:   DESC,
			wantVal: -1,
			kind:    reflect.Uint64,
		},
		{
			name:    "uint64 equal",
			values:  []any{uint64(2), uint64(2)},
			order:   DESC,
			wantVal: 0,
			kind:    reflect.Uint64,
		},
		{
			name:    "float32 ASC 1,2",
			values:  []any{float32(1.1), float32(2.1)},
			order:   ASC,
			wantVal: -1,
			kind:    reflect.Float32,
		},
		{
			name:    "float32 DESC 1,2",
			values:  []any{float32(1.1), float32(2.1)},
			order:   DESC,
			wantVal: 1,
			kind:    reflect.Float32,
		},
		{
			name:    "float32 ASC 2,1",
			values:  []any{float32(2), float32(1)},
			order:   ASC,
			wantVal: 1,
			kind:    reflect.Float32,
		},
		{
			name:    "float32 DESC 2,1",
			values:  []any{float32(2.1), float32(1.1)},
			order:   DESC,
			wantVal: -1,
			kind:    reflect.Float32,
		},
		{
			name:    "float32 equal",
			values:  []any{float32(2.1), float32(2.1)},
			order:   DESC,
			wantVal: 0,
			kind:    reflect.Float32,
		},
		{
			name:    "float64 ASC 1,2",
			values:  []any{float64(1.1), float64(2.1)},
			order:   ASC,
			wantVal: -1,
			kind:    reflect.Float64,
		},
		{
			name:    "float64 DESC 1,2",
			values:  []any{float64(1), float64(2)},
			order:   DESC,
			wantVal: 1,
			kind:    reflect.Float64,
		},
		{
			name:    "float64 ASC 2,1",
			values:  []any{float64(2), float64(1)},
			order:   ASC,
			wantVal: 1,
			kind:    reflect.Float64,
		},
		{
			name:    "float64 DESC 2,1",
			values:  []any{float64(2.1), float64(1.1)},
			order:   DESC,
			wantVal: -1,
			kind:    reflect.Float64,
		},
		{
			name:    "float64 equal",
			values:  []any{float64(2.1), float64(2.1)},
			order:   DESC,
			wantVal: 0,
			kind:    reflect.Float64,
		},
		{
			name:    "string equal",
			values:  []any{"x", "x"},
			order:   DESC,
			wantVal: 0,
			kind:    reflect.String,
		},
	}
	for _, tc := range testcases {
		ms.T().Run(tc.name, func(t *testing.T) {
			cmp, ok := compareFuncMapping[tc.kind]
			require.True(t, ok)
			val := cmp(tc.values[0], tc.values[1], tc.order)
			assert.Equal(t, tc.wantVal, val)
		})
	}
}
