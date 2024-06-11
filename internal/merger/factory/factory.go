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
	"log"
	"strings"

	"github.com/ecodeclub/ekit/slice"
	"github.com/ecodeclub/eorm/internal/merger"
	"github.com/ecodeclub/eorm/internal/merger/internal/aggregatemerger"
	"github.com/ecodeclub/eorm/internal/merger/internal/aggregatemerger/aggregator"
	"github.com/ecodeclub/eorm/internal/merger/internal/batchmerger"
	"github.com/ecodeclub/eorm/internal/merger/internal/groupbymerger"
	"github.com/ecodeclub/eorm/internal/merger/internal/pagedmerger"
	"github.com/ecodeclub/eorm/internal/merger/internal/sortmerger"
	"github.com/ecodeclub/eorm/internal/query"
	"github.com/ecodeclub/eorm/internal/rows"
)

var (
	ErrInvalidColumnInfo          = errors.New("factory: ColumnInfo非法")
	ErrEmptyColumnList            = errors.New("factory: 列列表为空")
	ErrColumnNotFoundInSelectList = errors.New("factory: Select列表中未找到列")
	ErrInvalidLimit               = errors.New("factory: Limit小于1")
	ErrInvalidOffset              = errors.New("factory: Offset不等于0")
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

	if err := q.validateSelect(); err != nil {
		return err
	}

	if err := q.validateGroupBy(); err != nil {
		return err
	}

	if err := q.validateOrderBy(); err != nil {
		return err
	}

	if err := q.validateLimit(); err != nil {
		return err
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
		c.Order = merger.DESC
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
		// 清除ASC
		c.Order = merger.DESC
		if !slice.Contains(q.Select, c) {
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

func newAggregateMerger(origin, target QuerySpec) (merger.Merger, error) {
	aggregators := getAggregators(origin, target)
	log.Printf("aggregators = %#v\n", aggregators)
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
			log.Printf("min index = %d\n", c.Index)
		case "MAX":
			aggregators = append(aggregators, aggregator.NewMax(c))
			log.Printf("max index = %d\n", c.Index)
		case "AVG":
			aggregators = append(aggregators, aggregator.NewAVG(c, target.Select[i+1], target.Select[i+2]))
			i += 2
			log.Printf("avg index = %d\n", c.Index)
		case "SUM":
			aggregators = append(aggregators, aggregator.NewSum(c))
			log.Printf("sum index = %d\n", c.Index)
		case "COUNT":
			aggregators = append(aggregators, aggregator.NewCount(c))
			log.Printf("count index = %d\n", c.Index)
		}
	}
	return aggregators
}

func newGroupByMergerWithoutHaving(origin, target QuerySpec) (merger.Merger, error) {
	aggregators := getAggregators(origin, target)
	log.Printf("groupby aggregators = %#v\n", aggregators)
	return groupbymerger.NewAggregatorMerger(aggregators, target.GroupBy), nil
}

func newOrderByMerger(origin, target QuerySpec) (merger.Merger, error) {
	var columns []sortmerger.SortColumn
	for i := 0; i < len(target.OrderBy); i++ {
		c := target.OrderBy[i]
		if i < len(origin.OrderBy) && strings.ToUpper(origin.OrderBy[i].AggregateFunc) == "AVG" {
			s := sortmerger.NewSortColumn(origin.OrderBy[i].SelectName(), sortmerger.Order(origin.OrderBy[i].Order))
			columns = append(columns, s)
			i++
			continue
		}
		s := sortmerger.NewSortColumn(c.SelectName(), sortmerger.Order(c.Order))
		columns = append(columns, s)
	}

	var isPreScanAll bool
	if slice.Contains(target.Features, query.GroupBy) {
		isPreScanAll = true
	}

	log.Printf("sortColumns = %#v\n", columns)
	return sortmerger.NewMerger(isPreScanAll, columns...)
}

func New(origin, target QuerySpec) (merger.Merger, error) {
	for _, spec := range []QuerySpec{origin, target} {
		if err := spec.Validate(); err != nil {
			return nil, err
		}
	}
	var mp = map[query.Feature]newMergerFunc{
		query.AggregateFunc: newAggregateMerger,
		query.GroupBy:       newGroupByMergerWithoutHaving,
		query.OrderBy:       newOrderByMerger,
	}
	var mergers []merger.Merger
	for _, feature := range target.Features {
		switch feature {
		case query.AggregateFunc, query.GroupBy, query.OrderBy:
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
		}
	}
	if len(mergers) == 0 {
		mergers = append(mergers, batchmerger.NewMerger())
	}
	log.Printf("mergers = %#v\n", mergers)
	return &MergerPipeline{mergers: mergers}, nil
}

type MergerPipeline struct {
	mergers []merger.Merger
}

func (m *MergerPipeline) Merge(ctx context.Context, results []rows.Rows) (rows.Rows, error) {
	r, err := m.mergers[0].Merge(ctx, results)
	if err != nil {
		return nil, err
	}
	if len(m.mergers) == 1 {
		return r, nil
	}
	columns, _ := r.Columns()
	log.Printf("pipline merge[0] columns = %#v\n", columns)
	for _, mg := range m.mergers[1:] {
		r, err = mg.Merge(ctx, []rows.Rows{r})
		if err != nil {
			return nil, err
		}
		c, _ := r.Columns()
		log.Printf("pipline merge[1:] columns = %#v\n", c)
	}
	return r, nil
}

// NewBatchMerger 仅供sharding_select通过测试使用,后续重构并删掉该方法并只保留上方New方法
func NewBatchMerger() (merger.Merger, error) {
	return batchmerger.NewMerger(), nil
}
