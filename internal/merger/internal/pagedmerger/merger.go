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

package pagedmerger

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"github.com/ecodeclub/eorm/internal/rows"

	"github.com/ecodeclub/eorm/internal/merger"
	"github.com/ecodeclub/eorm/internal/merger/internal/errs"
)

type Merger struct {
	m      merger.Merger
	limit  int
	offset int
}

func NewMerger(m merger.Merger, offset int, limit int) (*Merger, error) {
	if offset < 0 || limit <= 0 {
		return nil, errs.ErrMergerInvalidLimitOrOffset
	}

	return &Merger{
		m:      m,
		limit:  limit,
		offset: offset,
	}, nil
}

func (m *Merger) Merge(ctx context.Context, results []rows.Rows) (rows.Rows, error) {
	rs, err := m.m.Merge(ctx, results)
	if err != nil {
		return nil, err
	}
	err = m.nextOffset(ctx, rs)
	if err != nil {
		return nil, err
	}
	return &Rows{
		rows:  rs,
		mu:    &sync.RWMutex{},
		limit: m.limit,
	}, nil
}

// nextOffset 会把游标挪到 offset 所指定的位置。
func (m *Merger) nextOffset(ctx context.Context, rows rows.Rows) error {
	offset := m.offset
	for i := 0; i < offset; i++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// 如果偏移量超过rows结果集返回的行数，不会报错。用户最终查到0行
		if !rows.Next() {
			return rows.Err()
		}
	}
	return nil
}

type Rows struct {
	rows    rows.Rows
	limit   int
	cnt     int
	lastErr error
	closed  bool
	mu      *sync.RWMutex
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
	if r.cnt >= r.limit || r.lastErr != nil {
		r.mu.Unlock()
		_ = r.Close()
		return false
	}
	canNext, err := r.nextVal()
	if err != nil {
		r.lastErr = err
		r.mu.Unlock()
		_ = r.Close()
		return false
	}
	if !canNext {
		r.mu.Unlock()
		_ = r.Close()
		return canNext
	}
	r.cnt++
	r.mu.Unlock()
	return canNext
}
func (r *Rows) nextVal() (bool, error) {
	if r.rows.Next() {
		return true, nil
	}
	if r.rows.Err() != nil {
		return false, r.rows.Err()
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
	return r.rows.Scan(dest...)
}

func (r *Rows) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	return r.rows.Close()
}

func (r *Rows) ColumnTypes() ([]*sql.ColumnType, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, fmt.Errorf("%w", errs.ErrMergerRowsClosed)
	}
	return r.rows.ColumnTypes()
}
func (r *Rows) Columns() ([]string, error) {
	return r.rows.Columns()
}

func (r *Rows) Err() error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.lastErr
}
