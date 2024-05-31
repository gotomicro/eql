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

package sortmerger

import (
	"database/sql/driver"
	"reflect"
	"time"
)

var compareFuncMapping = map[reflect.Kind]func(any, any, Order) int{
	reflect.Int:     compare[int],
	reflect.Int8:    compare[int8],
	reflect.Int16:   compare[int16],
	reflect.Int32:   compare[int32],
	reflect.Int64:   compare[int64],
	reflect.Uint8:   compare[uint8],
	reflect.Uint16:  compare[uint16],
	reflect.Uint32:  compare[uint32],
	reflect.Uint64:  compare[uint64],
	reflect.Float32: compare[float32],
	reflect.Float64: compare[float64],
	reflect.String:  compare[string],
	reflect.Uint:    compare[uint],
}

type Heap struct {
	h           []*node
	sortColumns sortColumns
}

func (h *Heap) Len() int {
	return len(h.h)
}

func (h *Heap) Less(i, j int) bool {
	for k := 0; k < h.sortColumns.Len(); k++ {
		valueI := h.h[i].sortCols[k]
		valueJ := h.h[j].sortCols[k]
		_, ok := valueJ.(driver.Valuer)
		var cp func(any, any, Order) int
		if ok {
			cp = compareNullable
		} else {
			kind := reflect.TypeOf(valueI).Kind()
			cp = compareFuncMapping[kind]
		}
		res := cp(valueI, valueJ, h.sortColumns.Get(k).order)
		if res == 0 {
			continue
		}
		if res == -1 {
			return true
		}
		return false
	}
	return false
}

func (h *Heap) Swap(i, j int) {
	h.h[i], h.h[j] = h.h[j], h.h[i]
}

func (h *Heap) Push(x any) {
	h.h = append(h.h, x.(*node))
}

func (h *Heap) Pop() any {
	v := h.h[len(h.h)-1]
	h.h = h.h[:len(h.h)-1]
	return v
}

type node struct {
	index    int
	sortCols []any
	columns  []any
}

// 升序时， -1 表示 i < j, 1 表示i > j ,0 表示两者相同
// 降序时，-1 表示 i > j, 1 表示 i < j ,0 表示两者相同

func compare[T Ordered](ii any, jj any, order Order) int {
	i, j := ii.(T), jj.(T)
	if i < j && order == ASC || i > j && order == DESC {
		return -1
	} else if i > j && order == ASC || i < j && order == DESC {
		return 1
	} else {
		return 0
	}
}

func compareNullable(ii, jj any, order Order) int {
	i := ii.(driver.Valuer)
	j := jj.(driver.Valuer)
	iVal, _ := i.Value()
	jVal, _ := j.Value()
	// 如果i,j都为空返回0
	// 如果val返回为空永远是最小值
	if iVal == nil && jVal == nil {
		return 0
	} else if iVal == nil && order == ASC || jVal == nil && order == DESC {
		return -1
	} else if iVal == nil && order == DESC || jVal == nil && order == ASC {
		return 1
	}

	vali, ok := iVal.(time.Time)
	if ok {
		valj := jVal.(time.Time)
		return compare[int64](vali.UnixMilli(), valj.UnixMilli(), order)
	}
	kind := reflect.TypeOf(iVal).Kind()
	return compareFuncMapping[kind](iVal, jVal, order)
}
