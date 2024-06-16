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

func TestCount_Aggregate(t *testing.T) {
	testcases := []struct {
		name       string
		input      [][]any
		wantVal    any
		wantErr    error
		countIndex int
	}{
		{
			name: "count正常合并",
			input: [][]any{
				{
					int64(10),
				},
				{
					int64(20),
				},
				{
					int64(30),
				},
			},
			wantVal:    int64(60),
			countIndex: 0,
		},
		{
			name: "传入的参数非AggregateElement类型",
			input: [][]any{
				{
					"1",
				},
				{
					"3",
				},
			},
			wantErr:    errs.ErrMergerAggregateFuncNotFound,
			countIndex: 0,
		},
		{
			name: "columnInfo的index不合法",
			input: [][]any{
				{
					int64(10),
				},
				{
					int64(20),
				},
				{
					int64(30),
				},
			},
			countIndex: 20,
			wantErr:    errs.ErrMergerInvalidAggregateColumnIndex,
		},
		{
			name: "columnInfo为nullable类型",
			input: [][]any{
				{
					sql.NullInt64{
						Int64: 4,
						Valid: true,
					},
				},
				{
					sql.NullFloat64{
						Valid: false,
					},
				},
				{
					sql.NullInt64{
						Valid: true,
						Int64: 7,
					},
				},
			},
			countIndex: 0,
			wantVal:    int64(11),
		},
		{
			name: "所有列查出来的都为null",
			input: [][]any{
				{
					sql.NullInt64{
						Valid: false,
					},
				},
				{
					sql.NullInt64{
						Valid: false,
					},
				},
				{
					sql.NullInt64{
						Valid: false,
					},
				},
			},
			countIndex: 0,
			wantVal: sql.NullInt64{
				Valid: false,
			},
		},
		{
			name: "所有列查出来的都不是null",
			input: [][]any{
				{
					sql.NullInt64{
						Int64: 8,
						Valid: true,
					},
				},
				{
					sql.NullInt64{
						Int64: 9,
						Valid: true,
					},
				},
				{
					sql.NullInt64{
						Valid: true,
						Int64: 8,
					},
				},
			},
			countIndex: 0,
			wantVal:    int64(25),
		},
		{
			name: "表示 三者混合情况",
			input: [][]any{
				{
					sql.NullInt64{
						Int64: 8,
						Valid: true,
					},
				},
				{
					sql.NullInt64{Valid: false},
				},
				{
					int64(8),
				},
			},
			countIndex: 0,
			wantVal:    int64(16),
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			info := merger.ColumnInfo{Index: tc.countIndex, Name: "id", AggregateFunc: "COUNT"}
			count := NewCount(info)
			val, err := count.Aggregate(tc.input)
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantVal, val)
			assert.Equal(t, info, count.ColumnInfo())
		})
	}

}
