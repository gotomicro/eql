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

func TestSum_Aggregate(t *testing.T) {
	testcases := []struct {
		name     string
		input    [][]any
		wantVal  any
		wantErr  error
		sumIndex int
	}{
		{
			name: "sum正常合并",
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
			wantVal:  int64(60),
			sumIndex: 0,
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
			wantErr:  errs.ErrMergerAggregateFuncNotFound,
			sumIndex: 0,
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
			sumIndex: 20,
			wantErr:  errs.ErrMergerInvalidAggregateColumnIndex,
		},
		{
			name: "columnInfo为nullable类型",
			input: [][]any{
				{
					sql.NullFloat64{
						Float64: 2.2,
						Valid:   true,
					},
				},
				{
					sql.NullFloat64{
						Valid: false,
					},
				},
				{
					sql.NullFloat64{
						Valid:   true,
						Float64: 3.4,
					},
				},
			},
			sumIndex: 0,
			wantVal:  5.6,
		},
		{
			name: "所有列查出来的都为null",
			input: [][]any{
				{
					sql.NullFloat64{
						Valid: false,
					},
				},
				{
					sql.NullFloat64{
						Valid: false,
					},
				},
				{
					sql.NullFloat64{
						Valid: false,
					},
				},
			},
			sumIndex: 0,
			wantVal: sql.NullFloat64{
				Valid: false,
			},
		},
		{
			name: "所有列查出来的都不是null",
			input: [][]any{
				{
					sql.NullFloat64{
						Float64: 2.2,
						Valid:   true,
					},
				},
				{
					sql.NullFloat64{
						Float64: 5.6,
						Valid:   true,
					},
				},
				{
					sql.NullFloat64{
						Valid:   true,
						Float64: 3.4,
					},
				},
			},
			sumIndex: 0,
			wantVal:  11.2,
		},
		{
			name: "表示三者混合的情况",
			input: [][]any{
				{
					sql.NullFloat64{
						Float64: 2.2,
						Valid:   true,
					},
				},
				{
					5.6,
				},
				{
					sql.NullFloat64{
						Valid: false,
					},
				},
			},
			sumIndex: 0,
			wantVal:  7.8,
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			info := merger.ColumnInfo{Index: tc.sumIndex, Name: "id", AggregateFunc: "SUM"}
			sum := NewSum(info)
			val, err := sum.Aggregate(tc.input)
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantVal, val)
			assert.Equal(t, info, sum.ColumnInfo())
		})
	}

}
