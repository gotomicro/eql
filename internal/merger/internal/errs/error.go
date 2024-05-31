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

package errs

import (
	"errors"
	"fmt"
)

var (
	ErrEmptySortColumns                  = errors.New("merger: 排序列为空")
	ErrMergerEmptyRows                   = errors.New("merger: sql.Rows列表为空")
	ErrMergerRowsIsNull                  = errors.New("merger: sql.Rows列表中有元素为nil")
	ErrMergerScanNotNext                 = errors.New("merger: Scan之前没有调用Next方法")
	ErrMergerRowsClosed                  = errors.New("merger: Rows已经关闭")
	ErrMergerRowsDiff                    = errors.New("merger: sql.Rows列表中的字段不同")
	ErrMergerInvalidLimitOrOffset        = errors.New("merger: offset或limit小于0")
	ErrMergerAggregateHasEmptyRows       = errors.New("merger: 聚合函数计算时rowsList有一个或多个为空")
	ErrMergerInvalidAggregateColumnIndex = errors.New("merger: ColumnInfo的index不合法")
	ErrMergerAggregateFuncNotFound       = errors.New("merger: 聚合函数方法未找到")
	ErrMergerNotFound                    = errors.New("merger: merger未找到")
)

func NewRepeatSortColumn(column string) error {
	return fmt.Errorf("merger: 排序列重复%s", column)
}

func NewInvalidSortColumn(column string) error {
	return fmt.Errorf("merger: 数据库字段中没有这个排序列：%s", column)
}
