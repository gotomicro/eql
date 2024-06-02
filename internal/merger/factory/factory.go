package factory

import (
	"context"

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

type (
	// QuerySpec 解析SQL语句后可以较为容易得到的特征数据集合,各个具体merger初始化时所需要的参数的“并集”
	// 这里有几个要点:
	// 1. SQL的解析者能够比较容易创建Params
	// 2. 创建merger时,直接使用其中的字段或者只需稍加变换
	// 3. 不保留merger内部的知识,最好只与SQL标准耦合/关联
	QuerySpec struct {
		Features []query.Feature
		Select   []merger.ColumnInfo
		GroupBy  []merger.ColumnInfo
		OrderBy  []merger.ColumnInfo
		Limit    int
		Offset   int
	}
	// newMergerFunc 根据Params中的信息创建指定merger的工厂方法
	newMergerFunc func(q QuerySpec) (merger.Merger, error)
)

// funcMapping 充当决策表 key为预定义的sql特征或者特征的组合(或位运算) value最终选中的merger
// limit比较特殊未定义在决策表中,在下方New方法中先将limit特征去掉定位到merger再组合定位到merger返回组合后的Merger
var funcMapping = map[query.Feature]newMergerFunc{
	query.AggregateFunc: newAggregateMerger,
	query.GroupBy:       newGroupByMergerWithoutHaving,
	query.OrderBy:       newOrderByMerger,
}

func newBatchMerger(_ QuerySpec) (merger.Merger, error) {
	return batchmerger.NewMerger(), nil
}

func newAggregateMerger(q QuerySpec) (merger.Merger, error) {
	return aggregatemerger.NewMerger(), nil
}

func newGroupByMergerWithoutHaving(q QuerySpec) (merger.Merger, error) {
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
			agg = append(agg, aggregator.NewAVG(merger.NewColumnInfo(src.Index, src.Name), merger.NewColumnInfo(src.Index+1, src.Name), src.Name))
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

func newOrderByMerger(q QuerySpec) (merger.Merger, error) {
	s := slice.Map(q.OrderBy, func(idx int, src merger.ColumnInfo) sortmerger.SortColumn {
		return sortmerger.NewSortColumn(src.Name, sortmerger.Order(src.IsASCOrder))
	})
	return sortmerger.NewMerger(s...)
}

func newLimitMerger(m merger.Merger, p QuerySpec) (merger.Merger, error) {
	return pagedmerger.NewMerger(m, p.Offset, p.Limit)
}

func New(q QuerySpec) (merger.Merger, error) {
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
			m, err := mp[feature](q)
			if err != nil {
				return nil, err
			}
			mergers = append(mergers, m)
			prev = m
		case query.Limit:
			if prev == nil {
				prev = batchmerger.NewMerger()
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
