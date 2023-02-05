// Copyright 2021 gotomicro
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

	"github.com/gotomicro/eorm/internal/dialect"
	"github.com/gotomicro/eorm/internal/model"
	"github.com/gotomicro/eorm/internal/valuer"
)

type MasterSlavesDB struct {
	master   *sql.DB
	slaves   []*sql.DB
	getslave SlaveGeter
	core
}

func (m *MasterSlavesDB) getCore() core {
	return m.core
}

func (m *MasterSlavesDB) queryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	slave, err := m.getslave.Next(ctx)
	if err != nil {
		return nil, err
	}
	return slave.QueryContext(ctx, query, args)
}

func (m *MasterSlavesDB) execContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return m.master.ExecContext(ctx, query, args)
}

func (m *MasterSlavesDB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	tx, err := m.master.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &Tx{
		tx:   tx,
		db:   m.master,
		core: m.core,
	}, nil
}

func OpenMasterSlaveDB(driver string, master *sql.DB, opts ...MasterSlaveDBOption) (*MasterSlavesDB, error) {
	dl, err := dialect.Of(driver)
	if err != nil {
		return nil, err
	}
	orm := &MasterSlavesDB{
		core: core{
			metaRegistry: model.NewMetaRegistry(),
			dialect:      dl,
			// 可以设为默认，因为原本这里也有默认
			valCreator: valuer.BasicTypeCreator{
				Creator: valuer.NewUnsafeValue,
			},
		},
		master: master,
	}
	for _, o := range opts {
		o(orm)
	}
	return orm, nil
}

type MasterSlaveDBOption func(db *MasterSlavesDB)

func MasterSlaveWithSlave(slaves ...*sql.DB) MasterSlaveDBOption {
	return func(db *MasterSlavesDB) {
		db.slaves = slaves
	}
}

func MasterSlaveWithSlaveGeter(geter SlaveGeter) MasterSlaveDBOption {
	return func(db *MasterSlavesDB) {
		db.getslave = geter
	}
}
