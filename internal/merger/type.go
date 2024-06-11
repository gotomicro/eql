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

package merger

import (
	"context"
	"fmt"
	"strings"

	"github.com/ecodeclub/eorm/internal/rows"
)

// Merger 将sql.Rows列表里的元素合并，返回一个类似sql.Rows的迭代器
// Merger sql.Rows列表中每个sql.Rows仅支持单个结果集且每个sql.Rows中列集必须完全相同。
type Merger interface {
	Merge(ctx context.Context, results []rows.Rows) (rows.Rows, error)
}

type ColumnInfo struct {
	Index         int
	Name          string
	AggregateFunc string
	Alias         string
	ASC           bool
}

func (c ColumnInfo) SelectName() string {
	if c.Alias != "" {
		return c.Alias
	}
	if c.AggregateFunc != "" {
		return fmt.Sprintf("%s(%s)", c.AggregateFunc, c.Name)
	}
	return c.Name
}

func NewColumnInfo(index int, name string) ColumnInfo {
	return ColumnInfo{
		Index: index,
		Name:  name,
	}
}

func (c ColumnInfo) Validate() bool {
	// ColumnInfo.Name中不能包含括号,也就是聚合函数, name = `id`, 而不是name = count(`id`)
	// 聚合函数需要写在aggregateFunc字段中
	return !strings.Contains(c.Name, "(")
}
