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

package eorm

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/ecodeclub/eorm/internal/datasource/single"

	"github.com/ecodeclub/eorm/internal/valuer"
	_ "github.com/mattn/go-sqlite3"
)

func ExampleMiddleware() {
	db, err := single.OpenDB("sqlite3", "file:test.db?cache=shared&mode=memory")
	if err != nil {
		panic(err)
	}
	orm, _ := Open("sqlite3", db,
		DBWithMiddlewares(func(next HandleFunc) HandleFunc {
			return func(ctx context.Context, queryContext *QueryContext) *QueryResult {
				return &QueryResult{Result: "mdl1"}
			}
		}, func(next HandleFunc) HandleFunc {
			return func(ctx context.Context, queryContext *QueryContext) *QueryResult {
				return &QueryResult{Result: "mdl2"}
			}
		}))
	defer func() {
		_ = db.Close()
	}()
	fmt.Println(len(orm.ms))
	// Output:
	// 2
}

// TODO tx 是否要维护 *sql.DB
func ExampleDB_BeginTx() {
	db, _ := single.MemoryDB()
	tx, err := db.BeginTx(context.Background(), &sql.TxOptions{})
	if err == nil {
		fmt.Println("Begin")
	}
	orm, _ := Open("mysql", tx)
	defer func() {
		_ = orm.Close()
	}()
	// 或者 tx.Rollback()
	err = tx.Commit()
	if err == nil {
		fmt.Println("Commit")
	}
	// Output:
	// Begin
	// Commit
}

func ExampleOpen() {
	// case1 without DBOption
	db, err := single.OpenDB("sqlite3", "file:test.db?cache=shared&mode=memory")
	if err != nil {
		panic(err)
	}
	orm, _ := Open("sqlite3", db)
	fmt.Printf("case1 dialect: %s\n", orm.dialect.Name)

	// Output:
	// case1 dialect: SQLite
}

func ExampleNewDeleter() {
	db := memoryDB()
	tm := &TestModel{}
	query, _ := NewDeleter[TestModel](db).From(tm).Build()
	fmt.Printf("SQL: %s", query.SQL)
	// Output:
	// SQL: DELETE FROM `test_model`;
}

func ExampleNewUpdater() {
	db := memoryDB()
	tm := &TestModel{
		Age: 18,
	}
	query, _ := NewUpdater[TestModel](db).Update(tm).Build()
	fmt.Printf("SQL: %s", query.SQL)
	// Output:
	// SQL: UPDATE `test_model` SET `id`=?,`first_name`=?,`age`=?,`last_name`=?;
}

// memoryDB 返回一个基于内存的 ORM，它使用的是 sqlite3 内存模式。
func memoryDB() *DB {
	db, err := single.MemoryDB()
	if err != nil {
		panic(err)
	}
	orm, err := Open("sqlite3", db)
	if err != nil {
		panic(err)
	}
	return orm
}

func memoryDBWithDB(dbName string) *DB {
	db, err := single.OpenDB("sqlite3", fmt.Sprintf("file:%s.db?cache=shared&mode=memory", dbName))
	if err != nil {
		panic(err)
	}
	orm, err := Open("sqlite3", db)
	if err != nil {
		panic(err)
	}
	return orm
}

// go test -bench=BenchmarkQuerier_Get -benchmem -benchtime=10000x
// goos: linux
// goarch: amd64
// pkg: github.com/ecodeclub/eorm
// cpu: Intel(R) Core(TM) i5-10400F CPU @ 2.90GHz
// BenchmarkQuerier_Get/unsafe-12             10000            446263 ns/op            3849 B/op        116 allocs/op
// BenchmarkQuerier_Get/reflect-12            10000            854557 ns/op            4062 B/op        128 allocs/op
// PASS
// ok      github.com/ecodeclub/eorm       13.072s
func BenchmarkQuerier_Get(b *testing.B) {
	b.ReportAllocs()
	orm := memoryDBWithDB("benchmarkQuerierGet")
	defer func() {
		_ = orm.Close()
	}()
	_ = RawQuery[any](orm, TestModel{}.CreateSQL()).Exec(context.Background())
	res := NewInserter[TestModel](orm).Values(&TestModel{
		Id:        12,
		FirstName: "Deng",
		Age:       18,
		LastName:  &sql.NullString{String: "Ming", Valid: true},
	}).Exec(context.Background())
	if res.Err() != nil {
		b.Fatal(res.Err())
	}
	affected, err := res.RowsAffected()
	if err != nil {
		b.Fatal(err)
	}
	if affected == 0 {
		b.Fatal()
	}

	b.Run("unsafe", func(b *testing.B) {
		orm.valCreator = valuer.BasicTypeCreator{
			Creator: valuer.NewUnsafeValue,
		}
		for i := 0; i < b.N; i++ {
			_, err = NewSelector[TestModel](orm).From(TableOf(&TestModel{}, "t1")).Get(context.Background())
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("reflect", func(b *testing.B) {
		orm.valCreator = valuer.BasicTypeCreator{
			Creator: valuer.NewReflectValue,
		}
		for i := 0; i < b.N; i++ {
			_, err = NewSelector[TestModel](orm).From(TableOf(&TestModel{}, "t1")).Get(context.Background())
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
