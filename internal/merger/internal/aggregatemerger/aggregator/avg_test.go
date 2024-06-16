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

package aggregator

import (
	"database/sql"
	"testing"

	"github.com/ecodeclub/eorm/internal/merger"

	"github.com/ecodeclub/eorm/internal/merger/internal/errs"

	"github.com/stretchr/testify/assert"
)

func TestAvg_Aggregate(t *testing.T) {
	testcases := []struct {
		name    string
		input   [][]any
		index   []int
		wantVal any
		wantErr error
	}{
		{
			name: "avg正常合并",
			input: [][]any{
				{
					float64(10) / float64(2),
					int64(10),
					int64(2),
				},
				{
					float64(20) / float64(2),
					int64(20),
					int64(2),
				},
				{
					float64(30) / float64(2),
					int64(30),
					int64(2),
				},
			},
			index:   []int{0, 1, 2},
			wantVal: float64(10),
		},
		{
			name: "传入的参数非AggregateElement类型",
			input: [][]any{
				{
					"1.5",
					"1",
					"2",
				},
				{
					"0.75",
					"3",
					"4",
				},
			},
			index:   []int{0, 1, 2},
			wantErr: errs.ErrMergerAggregateFuncNotFound,
		},
		{
			name: "columnInfo的index不合法",
			input: [][]any{
				{
					int64(10),
					int64(2),
				},
				{
					int64(20),
					int64(2),
				},
			},
			index:   []int{0, 3, 10},
			wantErr: errs.ErrMergerInvalidAggregateColumnIndex,
		},
		{
			name: "count和sum均为nullable类型",
			input: [][]any{
				{
					sql.NullFloat64{
						Valid: false,
					},
					sql.NullInt64{
						Valid: false,
					},
					sql.NullInt64{
						Valid: true,
						Int64: 4,
					},
				},
				{
					sql.NullFloat64{
						Valid: false,
					},
					sql.NullInt64{
						Valid: false,
					},
					sql.NullInt64{
						Valid: true,
						Int64: 4,
					},
				},
				{
					sql.NullFloat64{
						Valid: false,
					},
					sql.NullInt64{
						Valid: true,
						Int64: 24,
					},
					sql.NullInt64{
						Valid: true,
						Int64: 4,
					},
				},
			},
			index:   []int{0, 1, 2},
			wantVal: float64(2),
		},
		{
			name: "count和sum既有nullable又有nullable类型的",
			input: [][]any{
				{
					sql.NullFloat64{
						Valid: false,
					},
					sql.NullInt64{
						Valid: true,
						Int64: 4,
					},
					sql.NullInt64{
						Valid: true,
						Int64: 4,
					},
				},
				{
					sql.NullFloat64{
						Valid: false,
					},
					int64(4),
					int64(4),
				},
				{
					sql.NullFloat64{
						Valid: false,
					},
					sql.NullInt64{
						Valid: false,
					},
					sql.NullInt64{
						Valid: false,
					},
				},
			},
			index:   []int{0, 1, 2},
			wantVal: float64(1),
		},
		{
			name: "sum所有列都为nil",
			input: [][]any{
				{
					sql.NullFloat64{
						Valid: false,
					},
					sql.NullInt64{
						Valid: false,
					},
					sql.NullInt64{
						Valid: true,
						Int64: 4,
					},
				},
				{
					sql.NullFloat64{
						Valid: false,
					},
					sql.NullInt64{
						Valid: false,
					},
					int64(4),
				},
				{
					sql.NullFloat64{
						Valid: false,
					},
					sql.NullInt64{
						Valid: false,
					},
					sql.NullInt64{
						Valid: false,
					},
				},
			},
			index: []int{0, 1, 2},
			wantVal: sql.NullFloat64{
				Valid: false,
			},
		},
		{
			name: "count所有列都为nil",
			input: [][]any{
				{
					sql.NullFloat64{
						Valid: false,
					},
					sql.NullInt64{
						Valid: true,
						Int64: 4,
					},
					sql.NullInt64{
						Valid: false,
					},
				},
				{
					sql.NullFloat64{
						Valid: false,
					},
					sql.NullInt64{
						Valid: true,
						Int64: 4,
					},
					sql.NullInt64{
						Valid: false,
					},
				},
				{
					sql.NullFloat64{
						Valid: false,
					},
					sql.NullInt64{
						Valid: false,
					},
					sql.NullInt64{
						Valid: false,
					},
				},
			},
			index: []int{0, 1, 2},
			wantVal: sql.NullFloat64{
				Valid: false,
			},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			avgColumnInfo := merger.ColumnInfo{Index: tc.index[0], Name: "`grade`", AggregateFunc: "AVG"}
			avg := NewAVG(avgColumnInfo,
				merger.ColumnInfo{Index: tc.index[1], Name: "`grade`", AggregateFunc: "SUM"},
				merger.ColumnInfo{Index: tc.index[2], Name: "`grade`", AggregateFunc: "COUNT"})
			val, err := avg.Aggregate(tc.input)
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantVal, val)
			assert.Equal(t, avgColumnInfo, avg.ColumnInfo())
		})
	}

}
