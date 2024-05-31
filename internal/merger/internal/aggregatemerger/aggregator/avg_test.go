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
					int64(10),
					int64(2),
				},
				{
					int64(20),
					int64(2),
				},
				{
					int64(30),
					int64(2),
				},
			},
			index:   []int{0, 1},
			wantVal: float64(10),
		},
		{
			name: "传入的参数非AggregateElement类型",
			input: [][]any{
				{
					"1",
					"2",
				},
				{
					"3",
					"4",
				},
			},
			index:   []int{0, 1},
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
			index:   []int{0, 10},
			wantErr: errs.ErrMergerInvalidAggregateColumnIndex,
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			avg := NewAVG(merger.NewColumnInfo(tc.index[0], "SUM(grade)"), merger.NewColumnInfo(tc.index[1], "COUNT(grade)"), "AVG(grade)")
			val, err := avg.Aggregate(tc.input)
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantVal, val)
			assert.Equal(t, "AVG(grade)", avg.ColumnName())
		})
	}

}
