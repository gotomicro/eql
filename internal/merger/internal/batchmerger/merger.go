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

package batchmerger

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"github.com/ecodeclub/eorm/internal/rows"

	"go.uber.org/multierr"

	"github.com/ecodeclub/eorm/internal/merger/internal/errs"
)

type Merger struct {
	cols []string
}

func NewMerger() *Merger {
	return &Merger{}
}

func (m *Merger) Merge(ctx context.Context, results []rows.Rows) (rows.Rows, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if len(results) == 0 {
		return nil, errs.ErrMergerEmptyRows
	}
	for i := 0; i < len(results); i++ {
		err := m.checkColumns(results[i])
		if err != nil {
			return nil, err
		}
	}
	return &Rows{
		rowsList: results,
		mu:       &sync.RWMutex{},
		columns:  m.cols,
	}, nil
}

// checkColumns 检查sql.Rows列表中sql.Rows的列集是否相同,并且sql.Rows不能为nil
func (m *Merger) checkColumns(rows rows.Rows) error {
	if rows == nil {
		return errs.ErrMergerRowsIsNull
	}
	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	if len(m.cols) == 0 {
		m.cols = cols
	}
	if len(m.cols) != len(cols) {
		return errs.ErrMergerRowsDiff
	}
	for idx, colName := range cols {
		if m.cols[idx] != colName {
			return errs.ErrMergerRowsDiff
		}
	}
	return nil

}

type Rows struct {
	rowsList []rows.Rows
	cnt      int
	mu       *sync.RWMutex
	columns  []string
	closed   bool
	lastErr  error
}

func (r *Rows) ColumnTypes() ([]*sql.ColumnType, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, fmt.Errorf("%w", errs.ErrMergerRowsClosed)
	}
	return r.rowsList[0].ColumnTypes()
}

func (*Rows) NextResultSet() bool {
	return false
}

func (r *Rows) Next() bool {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return false
	}
	if r.cnt >= len(r.rowsList) || r.lastErr != nil {
		r.mu.Unlock()
		_ = r.Close()
		return false
	}
	canNext, err := r.nextRows()
	if err != nil {
		r.lastErr = err
		r.mu.Unlock()
		_ = r.Close()
		return false
	}
	r.mu.Unlock()
	return canNext

}

func (r *Rows) nextRows() (bool, error) {
	row := r.rowsList[r.cnt]

	if row.Next() {
		return true, nil
	}

	for row.NextResultSet() {
		if row.Next() {
			return true, nil
		}
	}

	if row.Err() != nil {
		return false, row.Err()
	}

	for {
		r.cnt++
		if r.cnt >= len(r.rowsList) {
			break
		}
		row = r.rowsList[r.cnt]

		if row.Next() {
			return true, nil
		} else if row.Err() != nil {
			return false, row.Err()
		}

		for row.NextResultSet() {
			if row.Next() {
				return true, nil
			}
		}
	}
	return false, nil
}

func (r *Rows) Scan(dest ...any) error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.lastErr != nil {
		return r.lastErr
	}
	if r.closed {
		return errs.ErrMergerRowsClosed
	}
	return r.rowsList[r.cnt].Scan(dest...)

}

func (r *Rows) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	errorList := make([]error, 0, len(r.rowsList))
	for i := 0; i < len(r.rowsList); i++ {
		row := r.rowsList[i]
		err := row.Close()
		if err != nil {
			errorList = append(errorList, err)
		}
	}
	return multierr.Combine(errorList...)
}

func (r *Rows) Columns() ([]string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return nil, errs.ErrMergerRowsClosed
	}
	return r.columns, nil
}

func (r *Rows) Err() error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.lastErr
}
