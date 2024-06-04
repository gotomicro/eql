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
	"github.com/ecodeclub/eorm/internal/merger/internal/errs"
	"github.com/ecodeclub/eorm/internal/merger/internal/groupbymerger"
	"github.com/ecodeclub/eorm/internal/merger/internal/pagedmerger"
	"github.com/ecodeclub/eorm/internal/merger/internal/sortmerger"
	"github.com/ecodeclub/eorm/internal/query"
	"github.com/ecodeclub/eorm/internal/rows"
)

var (
	ErrEmptyColumnList = errors.New("字段列表为空")
)

type (
	// QuerySpec 解析SQL语句后可以较为容易得到的特征数据集合,各个具体merger初始化时所需要的参数的“并集”
	// 这里有几个要点:
	// 1. SQL的解析者能够比较容易创建Params
	// 2. 创建merger时,直接使用其中的字段或者只需稍加变换
	// 3. 不保留merger内部的知识,最好只与SQL标准耦合/关联
	// having
	QuerySpec struct {
		Features []query.Feature
		Select   []merger.ColumnInfo
		// [string][]merger.ColumnInfo ekit.LinkedHashmap
		GroupBy []merger.ColumnInfo
		OrderBy []merger.ColumnInfo
		Limit   int
		Offset  int
		// TODO: 只支持SELECT Distinct,暂不支持 COUNT(Distinct x)
	}
	// newMergerFunc 根据原始SQL的查询特征origin及目标SQL的查询特征target中的信息创建指定merger的工厂方法
	newMergerFunc func(origin, target QuerySpec) (merger.Merger, error)
)

func (q QuerySpec) Validate() error {
	// ColumnInfo.Name中不能包含括号,也就是聚合函数, name = `id`, 而不是name = count(`id`)
	// 聚合函数需要写在aggregateFunc字段中

	// Features 中含有orderBy, 则Orderby列不能为空
	return nil
}

func newAggregateMerger(origin, target QuerySpec) (merger.Merger, error) {
	var aggs []aggregator.Aggregator

	for i := 0; i < len(target.Select); i++ {
		c := target.Select[i]
		switch strings.ToUpper(c.AggregateFunc) {
		case "MIN":
			aggs = append(aggs, aggregator.NewMin(c))
			log.Printf("min index = %d\n", c.Index)
		case "MAX":
			aggs = append(aggs, aggregator.NewMax(c))
			log.Printf("max index = %d\n", c.Index)
		case "SUM":
			if i < len(origin.Select) && strings.ToUpper(origin.Select[i].AggregateFunc) == "AVG" {
				aggs = append(aggs, aggregator.NewAVG(c, target.Select[i+1], origin.Select[i].SelectName()))
				i += 1
				continue
			}
			aggs = append(aggs, aggregator.NewSum(c))
			log.Printf("sum index = %d\n", c.Index)
		case "COUNT":
			aggs = append(aggs, aggregator.NewCount(c))
			log.Printf("count index = %d\n", c.Index)
		}
	}
	// TODO: 当aggs为空时, 报不相关的错 merger: scan之前需要调用Next
	return aggregatemerger.NewMerger(aggs...), nil
}

func newGroupByMergerWithoutHaving(origin, q QuerySpec) (merger.Merger, error) {
	var agg []aggregator.Aggregator
	for _, src := range q.GroupBy {
		if src.AggregateFunc == "" {
			continue
		}
		switch src.AggregateFunc {
		case "sum":
			agg = append(agg, aggregator.NewSum(src))
		case "count":
			agg = append(agg, aggregator.NewCount(src))
		case "avg":
			// TODO 这里改写的时候Index如何设置? 是否会变?
			agg = append(agg, aggregator.NewAVG(src, src, src.Name))
		case "min":
			agg = append(agg, aggregator.NewMin(src))
		case "max":
			agg = append(agg, aggregator.NewMax(src))
		default:
			return nil, errs.ErrMergerAggregateFuncNotFound
		}
	}
	return groupbymerger.NewAggregatorMerger(agg, q.GroupBy), nil
}

func newOrderByMerger(origin, q QuerySpec) (merger.Merger, error) {
	s := slice.Map(q.OrderBy, func(idx int, src merger.ColumnInfo) sortmerger.SortColumn {
		log.Printf("col = %#v\n", src)
		return sortmerger.NewSortColumn(src.SelectName(), sortmerger.Order(src.ASC))
	})
	if len(s) == 0 {
		return nil, fmt.Errorf("orderby子句: %w", ErrEmptyColumnList)
	}
	return sortmerger.NewMerger(s...)
}

func newLimitMerger(m merger.Merger, p QuerySpec) (merger.Merger, error) {
	return pagedmerger.NewMerger(m, p.Offset, p.Limit)
}

func New(origin, q QuerySpec) (merger.Merger, error) {
	var mp = map[query.Feature]newMergerFunc{
		query.AggregateFunc: newAggregateMerger,
		query.GroupBy:       newGroupByMergerWithoutHaving,
		query.OrderBy:       newOrderByMerger,
	}
	var mergers []merger.Merger
	var prev merger.Merger
	for _, feature := range q.Features {
		switch feature {
		case query.AggregateFunc, query.GroupBy, query.OrderBy:
			m, err := mp[feature](origin, q)
			if err != nil {
				return nil, err
			}
			mergers = append(mergers, m)
		case query.Limit:
			if len(mergers) == 0 {
				prev = batchmerger.NewMerger()
			} else {
				prev = mergers[len(mergers)-1]
				mergers = mergers[:len(mergers)-1]
			}
			m, err := newLimitMerger(prev, q)
			if err != nil {
				return nil, err
			}
			mergers = append(mergers, m)
		}
	}

	if len(mergers) == 0 {
		mergers = append(mergers, batchmerger.NewMerger())
	}

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
	for _, mg := range m.mergers[1:] {
		r, err = mg.Merge(ctx, []rows.Rows{r})
		if err != nil {
			return nil, err
		}
	}
	return r, nil
}
