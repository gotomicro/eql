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

package factory

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/ecodeclub/ekit/slice"
	"github.com/ecodeclub/eorm/internal/merger"
	"github.com/ecodeclub/eorm/internal/merger/internal/aggregatemerger"
	"github.com/ecodeclub/eorm/internal/merger/internal/aggregatemerger/aggregator"
	"github.com/ecodeclub/eorm/internal/merger/internal/batchmerger"
	"github.com/ecodeclub/eorm/internal/merger/internal/distinctmerger"
	"github.com/ecodeclub/eorm/internal/merger/internal/groupbymerger"
	"github.com/ecodeclub/eorm/internal/merger/internal/pagedmerger"
	"github.com/ecodeclub/eorm/internal/merger/internal/sortmerger"
	"github.com/ecodeclub/eorm/internal/query"
	"github.com/ecodeclub/eorm/internal/rows"
)

var (
	ErrInvalidColumnInfo          = errors.New("merger: ColumnInfo非法")
	ErrEmptyColumnList            = errors.New("merger: 列列表为空")
	ErrColumnNotFoundInSelectList = errors.New("merger: Select列表中未找到列")
	ErrInvalidLimit               = errors.New("merger: Limit小于1")
	ErrInvalidOffset              = errors.New("merger: Offset不等于0")
	ErrInvalidFeatures            = errors.New("merger: Features非法")
)

type (
	// QuerySpec 解析SQL语句后可以较为容易得到的特征数据集合,各个具体merger初始化时所需要的参数的“并集”
	// 这里有几个要点:
	// 1. SQL的解析者能够比较容易创建QuerySpec
	// 2. 创建merger时,直接使用其中的字段或者只需稍加变换
	// 3. 不保留merger内部的知识,最好只与SQL标准耦合/关联
	QuerySpec struct {
		Features []query.Feature
		Select   []merger.ColumnInfo
		GroupBy  []merger.ColumnInfo
		OrderBy  []merger.ColumnInfo
		Limit    int
		Offset   int
		// TODO: 只支持SELECT Distinct,暂不支持 COUNT(Distinct x)
	}
	// newMergerFunc 根据原始SQL的查询特征origin及目标SQL的查询特征target中的信息创建指定merger的工厂方法
	newMergerFunc func(origin, target QuerySpec) (merger.Merger, error)
)

func (q QuerySpec) Validate() error {
	validateFuncs := []func() error{
		q.validateFeatures,
		q.validateSelect,
		q.validateGroupBy,
		q.validateDistinct,
		q.validateOrderBy,
		q.validateLimit,
	}
	for _, f := range validateFuncs {
		if err := f(); err != nil {
			return err
		}
	}
	return nil
}

func (q QuerySpec) validateFeatures() error {
	for i, v := range q.Features {
		if i == 0 {
			continue
		}
		if v < q.Features[i-1] {
			return fmt.Errorf("%w: 顺序错误", ErrInvalidFeatures)
		}
	}
	if slice.Contains(q.Features, query.AggregateFunc) && slice.Contains(q.Features, query.GroupBy) {
		return fmt.Errorf("%w: 聚合特征与GroupBy不该同时出现", ErrInvalidFeatures)
	}
	if slice.Contains(q.Features, query.GroupBy) && slice.Contains(q.Features, query.Distinct) {
		return fmt.Errorf("%w: GroupBy与DISTINCT不该同时出现", ErrInvalidFeatures)
	}
	return nil
}

func (q QuerySpec) validateSelect() error {
	if len(q.Select) == 0 {
		return fmt.Errorf("%w: select", ErrEmptyColumnList)
	}
	for i, c := range q.Select {
		if i != c.Index || !c.Validate() {
			return fmt.Errorf("%w: select %v", ErrInvalidColumnInfo, c.Name)
		}
	}
	return nil
}

func (q QuerySpec) validateGroupBy() error {
	if !slice.Contains(q.Features, query.GroupBy) {
		return nil
	}
	if len(q.GroupBy) == 0 {
		return fmt.Errorf("%w: groupby", ErrEmptyColumnList)
	}
	for _, c := range q.GroupBy {
		if !c.Validate() {
			return fmt.Errorf("%w: groupby %v", ErrInvalidColumnInfo, c.Name)
		}
		// 清除ASC
		c.Order = merger.OrderDESC
		if !slice.Contains(q.Select, c) {
			return fmt.Errorf("%w: groupby %v", ErrColumnNotFoundInSelectList, c.Name)
		}
	}
	for _, c := range q.Select {
		if c.AggregateFunc == "" && !slice.Contains(q.GroupBy, c) {
			return fmt.Errorf("%w: 非聚合列 %v 必须出现在groupby列表中", ErrInvalidColumnInfo, c.Name)
		}
		if c.AggregateFunc != "" && slice.Contains(q.GroupBy, c) {
			return fmt.Errorf("%w: 聚合列 %v 不能出现在groupby列表中", ErrInvalidColumnInfo, c.Name)
		}
	}
	return nil
}

func (q QuerySpec) validateDistinct() error {
	if !slice.Contains(q.Features, query.Distinct) {
		return nil
	}
	// 程序走到这q.Select的长度至少为1
	for _, c := range q.Select {
		// case2,3
		if !c.Distinct || !c.Validate() {
			return fmt.Errorf("%w: distinct %v", ErrInvalidColumnInfo, c.Name)
		}
	}
	return nil
}

func (q QuerySpec) validateOrderBy() error {
	if !slice.Contains(q.Features, query.OrderBy) {
		return nil
	}
	if len(q.OrderBy) == 0 {
		return fmt.Errorf("%w: orderby", ErrEmptyColumnList)
	}
	for _, c := range q.OrderBy {

		if !c.Validate() {
			return fmt.Errorf("%w: orderby %v", ErrInvalidColumnInfo, c.Name)
		}
		_, ok := slice.Find(q.Select, func(src merger.ColumnInfo) bool {
			return src.Index == c.Index && src.SelectName() == c.SelectName()
		})
		if !ok {
			return fmt.Errorf("%w: orderby %v", ErrColumnNotFoundInSelectList, c.Name)
		}
	}
	return nil
}

func (q QuerySpec) validateLimit() error {
	if !slice.Contains(q.Features, query.Limit) {
		return nil
	}
	if q.Limit < 1 {
		return fmt.Errorf("%w: limit=%d", ErrInvalidLimit, q.Limit)
	}

	if q.Offset != 0 {
		return fmt.Errorf("%w: offset=%d", ErrInvalidOffset, q.Offset)
	}

	return nil
}

// New 根据原SQL查询特征、目标SQL查询特征创建、组合merger的工厂方法
func New(origin, target QuerySpec) (merger.Merger, error) {
	for _, spec := range []QuerySpec{origin, target} {
		if err := spec.Validate(); err != nil {
			return nil, err
		}
	}
	var mp = map[query.Feature]newMergerFunc{
		query.AggregateFunc: newAggregateMerger,
		query.GroupBy:       newGroupByMergerWithoutHaving,
		query.Distinct:      newDistinctMerger,
		query.OrderBy:       newOrderByMerger,
	}
	var mergers []merger.Merger
	for _, feature := range target.Features {
		switch feature {
		case query.AggregateFunc, query.GroupBy, query.Distinct, query.OrderBy:
			m, err := mp[feature](origin, target)
			if err != nil {
				return nil, err
			}
			mergers = append(mergers, m)
		case query.Limit:
			var prev merger.Merger
			if len(mergers) == 0 {
				prev = batchmerger.NewMerger()
			} else {
				prev = mergers[len(mergers)-1]
				mergers = mergers[:len(mergers)-1]
			}
			m, err := pagedmerger.NewMerger(prev, target.Offset, target.Limit)
			if err != nil {
				return nil, err
			}
			mergers = append(mergers, m)
		default:
			return nil, fmt.Errorf("%w: feature: %d", ErrInvalidFeatures, feature)
		}
	}
	if len(mergers) == 0 {
		mergers = append(mergers, batchmerger.NewMerger())
	}
	return &pipeline{mergers: mergers}, nil
}

func newAggregateMerger(origin, target QuerySpec) (merger.Merger, error) {
	aggregators := getAggregators(origin, target)
	// TODO: 当aggs为空时, 报不相关的错 merger: scan之前需要调用Next
	return aggregatemerger.NewMerger(aggregators...), nil
}

func getAggregators(_, target QuerySpec) []aggregator.Aggregator {
	var aggregators []aggregator.Aggregator
	for i := 0; i < len(target.Select); i++ {
		c := target.Select[i]
		switch strings.ToUpper(c.AggregateFunc) {
		case "MIN":
			aggregators = append(aggregators, aggregator.NewMin(c))
		case "MAX":
			aggregators = append(aggregators, aggregator.NewMax(c))
		case "AVG":
			aggregators = append(aggregators, aggregator.NewAVG(c, target.Select[i+1], target.Select[i+2]))
			i += 2
		case "SUM":
			aggregators = append(aggregators, aggregator.NewSum(c))
		case "COUNT":
			aggregators = append(aggregators, aggregator.NewCount(c))
		}
	}
	return aggregators
}

func newGroupByMergerWithoutHaving(origin, target QuerySpec) (merger.Merger, error) {
	aggregators := getAggregators(origin, target)
	return groupbymerger.NewAggregatorMerger(aggregators, target.GroupBy), nil
}

func newDistinctMerger(_, target QuerySpec) (merger.Merger, error) {
	var sortColumns merger.SortColumns
	if len(target.OrderBy) != 0 {
		s, err := merger.NewSortColumns(target.OrderBy...)
		if err != nil {
			return nil, err
		}
		sortColumns = s
	}
	return distinctmerger.NewMerger(target.Select, sortColumns)
}

func newOrderByMerger(origin, target QuerySpec) (merger.Merger, error) {
	var columns []merger.ColumnInfo
	for i := 0; i < len(target.OrderBy); i++ {
		c := target.OrderBy[i]
		if i < len(origin.OrderBy) && strings.ToUpper(origin.OrderBy[i].AggregateFunc) == "AVG" {
			columns = append(columns, origin.OrderBy[i])
			i++
			continue
		}
		columns = append(columns, c)
	}

	var isPreScanAll bool
	if slice.Contains(target.Features, query.GroupBy) {
		isPreScanAll = true
	}
	return sortmerger.NewMerger(isPreScanAll, columns...)
}

type pipeline struct {
	mergers []merger.Merger
}

func (m *pipeline) Merge(ctx context.Context, results []rows.Rows) (rows.Rows, error) {
	r, err := m.mergers[0].Merge(ctx, results)
	if err != nil {
		return nil, err
	}
	if len(m.mergers) == 1 {
		return r, nil
	}
	for _, mg := range m.mergers[1:] {
		r, err = mg.Merge(ctx, []rows.Rows{r})
		if err != nil {
			return nil, err
		}
	}
	return r, nil
}

// NewBatchMerger 仅供sharding_select.go使用,后续重构后需要删掉该方法并只保留上方New方法
func NewBatchMerger() (merger.Merger, error) {
	return batchmerger.NewMerger(), nil
}
