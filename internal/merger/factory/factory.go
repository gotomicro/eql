package factory

import (
	"fmt"

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
)

type (
	// Params 解析SQL语句后可以较为容易得到的特征数据集合,各个具体merger初始化时所需要的参数的“并集”
	// 这里有几个要点:
	// 1. SQL的解析者能够比较容易创建Params
	// 2. 创建merger时,直接使用其中的字段或者只需稍加变换
	// 3. 不保留merger内部的知识,最好只与SQL标准耦合/关联
	Params struct {
		Select  []merger.ColumnInfo
		GroupBy []merger.ColumnInfo
		OrderBy []merger.ColumnInfo
		Limit   int
		Offset  int
	}
	// newMergerFunc 根据Params中的信息创建指定merger的工厂方法
	newMergerFunc func(p Params) (merger.Merger, error)
)

// funcMapping 充当决策表 key为预定义的sql特征或者特征的组合(或位运算) value最终选中的merger
// limit比较特殊未定义在决策表中,在下方New方法中先将limit特征去掉定位到merger再组合定位到merger返回组合后的Merger
var funcMapping = map[query.Feature]newMergerFunc{
	query.AggregateFunc:                 newAggregateMerger,
	query.GroupBy:                       newGroupByMergerWithoutHaving,
	query.AggregateFunc | query.GroupBy: newBatchMerger,
	query.OrderBy:                       newOrderByMerger,
}

func newBatchMerger(_ Params) (merger.Merger, error) {
	return batchmerger.NewMerger(), nil
}

func newAggregateMerger(p Params) (merger.Merger, error) {
	return aggregatemerger.NewMerger(), nil
}

func newGroupByMergerWithoutHaving(p Params) (merger.Merger, error) {
	var err error
	agg := slice.Map(p.GroupBy, func(idx int, src merger.ColumnInfo) aggregator.Aggregator {
		switch src.AggregateFunc {
		case "sum":
			return aggregator.NewSum(src)
		case "count":
			return aggregator.NewCount(src)
		case "avg":
			// TODO 这里改写的时候Index如何设置? 是否会变?
			return aggregator.NewAVG(merger.NewColumnInfo(src.Index, src.Name), merger.NewColumnInfo(src.Index+1, src.Name), src.Name)
		case "min":
			return aggregator.NewMin(src)
		case "max":
			return aggregator.NewMax(src)
		default:
			err = errs.ErrMergerAggregateFuncNotFound
			return nil
		}
	})
	if err != nil {
		return nil, err
	}
	return groupbymerger.NewAggregatorMerger(agg, p.GroupBy), nil
}

func newOrderByMerger(p Params) (merger.Merger, error) {
	s := slice.Map(p.OrderBy, func(idx int, src merger.ColumnInfo) sortmerger.SortColumn {
		return sortmerger.NewSortColumn(src.Name, sortmerger.Order(src.IsASCOrder))
	})
	return sortmerger.NewMerger(s...)
}

func newLimitMerger(m merger.Merger, p Params) (merger.Merger, error) {
	return pagedmerger.NewMerger(m, p.Offset, p.Limit)
}

func New(f query.Feature, p Params) (merger.Merger, error) {
	hasLimit := f&query.Limit != 0
	nonLimitFeatures := f &^ query.Limit
	newFunc, ok := funcMapping[nonLimitFeatures]
	if !ok {
		return nil, fmt.Errorf("%w: %v", errs.ErrMergerNotFound, f)
	}
	m, err := newFunc(p)
	if err != nil {
		return nil, err
	}
	if hasLimit {
		return newLimitMerger(m, p)
	}
	return m, nil
}
