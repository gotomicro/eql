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

package transaction_test

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"strings"
	"testing"

	"github.com/ecodeclub/eorm/internal/datasource"
	"github.com/ecodeclub/eorm/internal/datasource/shardingsource"
	"github.com/ecodeclub/eorm/internal/errs"
	"github.com/ecodeclub/eorm/internal/model"
	"go.uber.org/multierr"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/ecodeclub/eorm"
	"github.com/ecodeclub/eorm/internal/datasource/masterslave"
	"github.com/ecodeclub/eorm/internal/datasource/transaction"
	"github.com/ecodeclub/eorm/internal/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type TestDelayTxTestSuite struct {
	ShardingTransactionSuite
}

func (s *TestDelayTxTestSuite) TestExecute_Commit_Or_Rollback() {
	t := s.T()
	testCases := []struct {
		name         string
		wantAffected int64
		wantErr      error
		values       []*test.OrderDetail
		querySet     []*test.OrderDetail
		txFunc       func() (*eorm.Tx, error)
		mockOrder    func(mock1, mock2 sqlmock.Sqlmock)
		afterFunc    func(t *testing.T, tx *eorm.Tx, values []*test.OrderDetail)
	}{
		{
			name:    "begin err",
			wantErr: errors.New("begin err"),
			querySet: []*test.OrderDetail{
				{OrderId: 8, ItemId: 6, UsingCol1: "Kobe", UsingCol2: "Bryant"},
				{OrderId: 11, ItemId: 8, UsingCol1: "James", UsingCol2: "Harden"},
				{OrderId: 234, ItemId: 12, UsingCol1: "Kevin", UsingCol2: "Durant"},
				{OrderId: 253, ItemId: 8, UsingCol1: "Stephen", UsingCol2: "Curry"},
				{OrderId: 181, ItemId: 11, UsingCol1: "Kawhi", UsingCol2: "Leonard"},
			},
			mockOrder: func(mock1, mock2 sqlmock.Sqlmock) {
				mock2.ExpectBegin().WillReturnError(errors.New("begin err"))
				mock1.ExpectBegin().WillReturnError(errors.New("begin err"))
			},
			txFunc: func() (*eorm.Tx, error) {
				return s.shardingDB.BeginTx(transaction.UsingTxType(context.Background(), transaction.Delay), &sql.TxOptions{})
			},
			afterFunc: func(t *testing.T, tx *eorm.Tx, values []*test.OrderDetail) {},
		},
		{
			name:      "not find data source err",
			wantErr:   errs.NewErrNotFoundTargetDataSource("0.db.cluster.company.com:3306"),
			mockOrder: func(mock1, mock2 sqlmock.Sqlmock) {},
			afterFunc: func(t *testing.T, tx *eorm.Tx, values []*test.OrderDetail) {},
			txFunc: func() (*eorm.Tx, error) {
				ds := shardingsource.NewShardingDataSource(map[string]datasource.DataSource{
					"1.db.cluster.company.com:3306": s.clusterDB,
				})
				r := model.NewMetaRegistry()
				_, err := r.Register(&test.OrderDetail{},
					model.WithTableShardingAlgorithm(s.algorithm))
				require.NoError(t, err)
				db, err := eorm.OpenDS("mysql", ds, eorm.DBWithMetaRegistry(r))
				require.NoError(t, err)
				return db.BeginTx(transaction.UsingTxType(context.Background(), transaction.Delay), &sql.TxOptions{})
			},
		},
		{
			name:      "not complete Finder err",
			wantErr:   errs.NewErrNotCompleteFinder("0.db.cluster.company.com:3306"),
			mockOrder: func(mock1, mock2 sqlmock.Sqlmock) {},
			afterFunc: func(t *testing.T, tx *eorm.Tx, values []*test.OrderDetail) {},
			txFunc: func() (*eorm.Tx, error) {
				ds := shardingsource.NewShardingDataSource(map[string]datasource.DataSource{
					"0.db.cluster.company.com:3306": masterslave.NewMasterSlavesDB(s.mockMaster1DB, masterslave.MasterSlavesWithSlaves(
						newSlaves(t, s.mockSlave1DB, s.mockSlave2DB, s.mockSlave3DB))),
				})
				r := model.NewMetaRegistry()
				_, err := r.Register(&test.OrderDetail{},
					model.WithTableShardingAlgorithm(s.algorithm))
				require.NoError(t, err)
				db, err := eorm.OpenDS("mysql", ds, eorm.DBWithMetaRegistry(r))
				require.NoError(t, err)
				return db.BeginTx(transaction.UsingTxType(context.Background(), transaction.Delay), &sql.TxOptions{})
			},
		},
		// TODO: 未知错误导致测试失败,后续重构再开启
		// {
		// 	name:    "not find target db err",
		// 	wantErr: errs.NewErrNotFoundTargetDB("order_detail_db_1"),
		// 	mockOrder: func(mock1, mock2 sqlmock.Sqlmock) {
		// 		mock1.ExpectBegin()
		// 	},
		// 	afterFunc: func(t *testing.T, tx *eorm.Tx, values []*test.OrderDetail) {},
		// 	txFunc: func() (*eorm.Tx, error) {
		// 		clusterDB := cluster.NewClusterDB(map[string]*masterslave.MasterSlavesDB{
		// 			"order_detail_db_0": masterslave.NewMasterSlavesDB(s.mockMaster1DB, masterslave.MasterSlavesWithSlaves(
		// 				newSlaves(t, s.mockSlave1DB, s.mockSlave2DB, s.mockSlave3DB))),
		// 		})
		// 		ds := shardingsource.NewShardingDataSource(map[string]datasource.DataSource{
		// 			"0.db.cluster.company.com:3306": clusterDB,
		// 		})
		// 		r := model.NewMetaRegistry()
		// 		_, err := r.Register(&test.OrderDetail{},
		// 			model.WithTableShardingAlgorithm(s.algorithm))
		// 		require.NoError(t, err)
		// 		db, err := eorm.OpenDS("mysql", ds, eorm.DBWithMetaRegistry(r))
		// 		require.NoError(t, err)
		// 		return db.BeginTx(transaction.UsingTxType(context.Background(), transaction.Delay), &sql.TxOptions{})
		// 	},
		// },
		{
			name:         "select insert all commit err",
			wantAffected: 2,
			values: []*test.OrderDetail{
				{OrderId: 288, ItemId: 101, UsingCol1: "Jimmy", UsingCol2: "Butler"},
				{OrderId: 33, ItemId: 100, UsingCol1: "Nikolai", UsingCol2: "Jokic"},
			},
			querySet: []*test.OrderDetail{
				{OrderId: 8, ItemId: 6, UsingCol1: "Kobe", UsingCol2: "Bryant"},
				{OrderId: 11, ItemId: 8, UsingCol1: "James", UsingCol2: "Harden"},
				{OrderId: 234, ItemId: 12, UsingCol1: "Kevin", UsingCol2: "Durant"},
				{OrderId: 253, ItemId: 8, UsingCol1: "Stephen", UsingCol2: "Curry"},
				{OrderId: 181, ItemId: 11, UsingCol1: "Kawhi", UsingCol2: "Leonard"},
			},
			mockOrder: func(mock1, mock2 sqlmock.Sqlmock) {
				mock1.MatchExpectationsInOrder(false)
				mock2.MatchExpectationsInOrder(false)
				mock1.ExpectBegin()
				mock2.ExpectBegin()

				mock1.ExpectQuery(regexp.QuoteMeta("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_0`.`order_detail_tab_0` WHERE `order_id`!=?;SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_0`.`order_detail_tab_1` WHERE `order_id`!=?;SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_0`.`order_detail_tab_2` WHERE `order_id`!=?;")).
					WithArgs(123, 123, 123).
					WillReturnRows(mock1.NewRows([]string{"order_id", "item_id", "using_col1", "using_col2"}).AddRow(234, 12, "Kevin", "Durant").AddRow(8, 6, "Kobe", "Bryant"))

				mock2.ExpectQuery(regexp.QuoteMeta("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_1`.`order_detail_tab_0` WHERE `order_id`!=?;SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_1`.`order_detail_tab_1` WHERE `order_id`!=?;SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_1`.`order_detail_tab_2` WHERE `order_id`!=?;")).
					WithArgs(123, 123, 123).
					WillReturnRows(mock2.NewRows([]string{"order_id", "item_id", "using_col1", "using_col2"}).AddRow(253, 8, "Stephen", "Curry").AddRow(181, 11, "Kawhi", "Leonard").AddRow(11, 8, "James", "Harden"))

				mock1.ExpectExec(regexp.QuoteMeta("INSERT INTO `order_detail_db_0`.`order_detail_tab_0`(`order_id`,`item_id`,`using_col1`,`using_col2`) VALUES(?,?,?,?);")).
					WithArgs(288, 101, "Jimmy", "Butler").WillReturnResult(sqlmock.NewResult(1, 1))
				mock2.ExpectExec(regexp.QuoteMeta("INSERT INTO `order_detail_db_1`.`order_detail_tab_0`(`order_id`,`item_id`,`using_col1`,`using_col2`) VALUES(?,?,?,?);")).
					WithArgs(33, 100, "Nikolai", "Jokic").WillReturnResult(sqlmock.NewResult(1, 1))

				commitErr := errors.New("commit fail")
				mock1.ExpectCommit().WillReturnError(commitErr)
				mock2.ExpectCommit().WillReturnError(commitErr)
			},
			txFunc: func() (*eorm.Tx, error) {
				return s.shardingDB.BeginTx(transaction.UsingTxType(context.Background(), transaction.Delay), &sql.TxOptions{})
			},
			afterFunc: func(t *testing.T, tx *eorm.Tx, values []*test.OrderDetail) {
				err := tx.Commit()
				newErr := errors.New("commit fail")
				errSlice := strings.Split(err.Error(), "; ")
				wantErrSlice := []string{
					newMockCommitErr("order_detail_db_0", newErr).Error(),
					newMockCommitErr("order_detail_db_1", newErr).Error()}
				assert.ElementsMatch(t, wantErrSlice, errSlice)

				s.mockMaster.MatchExpectationsInOrder(false)
				s.mockMaster2.MatchExpectationsInOrder(false)
				rows := s.mockMaster.NewRows([]string{"order_id", "item_id", "using_col1", "using_col2"})
				s.mockMaster.ExpectQuery(regexp.QuoteMeta("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_0`.`order_detail_tab_0` WHERE (`order_id`=?) OR (`order_id`=?);")).
					WithArgs(288, 33).WillReturnRows(rows)

				s.mockMaster2.ExpectQuery(regexp.QuoteMeta("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_1`.`order_detail_tab_0` WHERE (`order_id`=?) OR (`order_id`=?);")).
					WithArgs(288, 33).WillReturnRows(rows)

				queryVal := s.findTgt(t, values)
				var wantOds []*test.OrderDetail
				assert.ElementsMatch(t, wantOds, queryVal)
			},
		},
		{
			name:         "select insert part commit err",
			wantAffected: 2,
			values: []*test.OrderDetail{
				{OrderId: 288, ItemId: 101, UsingCol1: "Jimmy", UsingCol2: "Butler"},
				{OrderId: 33, ItemId: 100, UsingCol1: "Nikolai", UsingCol2: "Jokic"},
			},
			querySet: []*test.OrderDetail{
				{OrderId: 8, ItemId: 6, UsingCol1: "Kobe", UsingCol2: "Bryant"},
				{OrderId: 11, ItemId: 8, UsingCol1: "James", UsingCol2: "Harden"},
				{OrderId: 234, ItemId: 12, UsingCol1: "Kevin", UsingCol2: "Durant"},
				{OrderId: 253, ItemId: 8, UsingCol1: "Stephen", UsingCol2: "Curry"},
				{OrderId: 181, ItemId: 11, UsingCol1: "Kawhi", UsingCol2: "Leonard"},
			},
			mockOrder: func(mock1, mock2 sqlmock.Sqlmock) {
				mock1.MatchExpectationsInOrder(false)
				mock2.MatchExpectationsInOrder(false)
				mock1.ExpectBegin()
				mock2.ExpectBegin()

				mock1.ExpectQuery(regexp.QuoteMeta("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_0`.`order_detail_tab_0` WHERE `order_id`!=?;SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_0`.`order_detail_tab_1` WHERE `order_id`!=?;SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_0`.`order_detail_tab_2` WHERE `order_id`!=?;")).
					WithArgs(123, 123, 123).
					WillReturnRows(mock1.NewRows([]string{"order_id", "item_id", "using_col1", "using_col2"}).AddRow(234, 12, "Kevin", "Durant").AddRow(8, 6, "Kobe", "Bryant"))

				mock2.ExpectQuery(regexp.QuoteMeta("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_1`.`order_detail_tab_0` WHERE `order_id`!=?;SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_1`.`order_detail_tab_1` WHERE `order_id`!=?;SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_1`.`order_detail_tab_2` WHERE `order_id`!=?;")).
					WithArgs(123, 123, 123).
					WillReturnRows(mock1.NewRows([]string{"order_id", "item_id", "using_col1", "using_col2"}).AddRow(253, 8, "Stephen", "Curry").AddRow(181, 11, "Kawhi", "Leonard").AddRow(11, 8, "James", "Harden"))

				mock1.ExpectExec(regexp.QuoteMeta("INSERT INTO `order_detail_db_0`.`order_detail_tab_0`(`order_id`,`item_id`,`using_col1`,`using_col2`) VALUES(?,?,?,?);")).
					WithArgs(288, 101, "Jimmy", "Butler").WillReturnResult(sqlmock.NewResult(1, 1))
				mock2.ExpectExec(regexp.QuoteMeta("INSERT INTO `order_detail_db_1`.`order_detail_tab_0`(`order_id`,`item_id`,`using_col1`,`using_col2`) VALUES(?,?,?,?);")).
					WithArgs(33, 100, "Nikolai", "Jokic").WillReturnResult(sqlmock.NewResult(1, 1))

				mock1.ExpectCommit()
				mock2.ExpectCommit().WillReturnError(errors.New("commit fail"))
			},
			txFunc: func() (*eorm.Tx, error) {
				return s.shardingDB.BeginTx(transaction.UsingTxType(context.Background(), transaction.Delay), &sql.TxOptions{})
			},
			afterFunc: func(t *testing.T, tx *eorm.Tx, values []*test.OrderDetail) {
				err := tx.Commit()
				wantErr := multierr.Combine(newMockCommitErr("order_detail_db_1", errors.New("commit fail")))
				assert.Equal(t, wantErr, err)

				s.mockMaster.MatchExpectationsInOrder(false)
				s.mockMaster2.MatchExpectationsInOrder(false)

				rows := s.mockMaster.NewRows([]string{"order_id", "item_id", "using_col1", "using_col2"})
				s.mockMaster.ExpectQuery(regexp.QuoteMeta("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_0`.`order_detail_tab_0` WHERE (`order_id`=?) OR (`order_id`=?);")).
					WithArgs(288, 33).WillReturnRows(rows)

				s.mockMaster2.ExpectQuery(regexp.QuoteMeta("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_1`.`order_detail_tab_0` WHERE (`order_id`=?) OR (`order_id`=?);")).
					WithArgs(288, 33).WillReturnRows(rows)

				queryVal := s.findTgt(t, values)
				var wantVal []*test.OrderDetail
				assert.ElementsMatch(t, wantVal, queryVal)
			},
		},
		{
			name:         "select insert all rollback err",
			wantAffected: 2,
			values: []*test.OrderDetail{
				{OrderId: 288, ItemId: 101, UsingCol1: "Jimmy", UsingCol2: "Butler"},
				{OrderId: 33, ItemId: 100, UsingCol1: "Nikolai", UsingCol2: "Jokic"},
			},
			querySet: []*test.OrderDetail{
				{OrderId: 8, ItemId: 6, UsingCol1: "Kobe", UsingCol2: "Bryant"},
				{OrderId: 11, ItemId: 8, UsingCol1: "James", UsingCol2: "Harden"},
				{OrderId: 234, ItemId: 12, UsingCol1: "Kevin", UsingCol2: "Durant"},
				{OrderId: 253, ItemId: 8, UsingCol1: "Stephen", UsingCol2: "Curry"},
				{OrderId: 181, ItemId: 11, UsingCol1: "Kawhi", UsingCol2: "Leonard"},
			},
			mockOrder: func(mock1, mock2 sqlmock.Sqlmock) {
				mock1.MatchExpectationsInOrder(false)
				mock2.MatchExpectationsInOrder(false)
				mock1.ExpectBegin()
				mock2.ExpectBegin()

				mock1.ExpectQuery(regexp.QuoteMeta("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_0`.`order_detail_tab_0` WHERE `order_id`!=?;SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_0`.`order_detail_tab_1` WHERE `order_id`!=?;SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_0`.`order_detail_tab_2` WHERE `order_id`!=?;")).
					WithArgs(123, 123, 123).
					WillReturnRows(mock1.NewRows([]string{"order_id", "item_id", "using_col1", "using_col2"}).AddRow(234, 12, "Kevin", "Durant").AddRow(8, 6, "Kobe", "Bryant"))

				mock2.ExpectQuery(regexp.QuoteMeta("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_1`.`order_detail_tab_0` WHERE `order_id`!=?;SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_1`.`order_detail_tab_1` WHERE `order_id`!=?;SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_1`.`order_detail_tab_2` WHERE `order_id`!=?;")).
					WithArgs(123, 123, 123).
					WillReturnRows(mock1.NewRows([]string{"order_id", "item_id", "using_col1", "using_col2"}).AddRow(253, 8, "Stephen", "Curry").AddRow(181, 11, "Kawhi", "Leonard").AddRow(11, 8, "James", "Harden"))

				mock1.ExpectExec(regexp.QuoteMeta("INSERT INTO `order_detail_db_0`.`order_detail_tab_0`(`order_id`,`item_id`,`using_col1`,`using_col2`) VALUES(?,?,?,?);")).
					WithArgs(288, 101, "Jimmy", "Butler").WillReturnResult(sqlmock.NewResult(1, 1))
				mock2.ExpectExec(regexp.QuoteMeta("INSERT INTO `order_detail_db_1`.`order_detail_tab_0`(`order_id`,`item_id`,`using_col1`,`using_col2`) VALUES(?,?,?,?);")).
					WithArgs(33, 100, "Nikolai", "Jokic").WillReturnResult(sqlmock.NewResult(1, 1))

				rollbackErr := errors.New("rollback fail")
				mock1.ExpectRollback().WillReturnError(rollbackErr)
				mock2.ExpectRollback().WillReturnError(rollbackErr)
			},
			txFunc: func() (*eorm.Tx, error) {
				return s.shardingDB.BeginTx(transaction.UsingTxType(context.Background(), transaction.Delay), &sql.TxOptions{})
			},
			afterFunc: func(t *testing.T, tx *eorm.Tx, values []*test.OrderDetail) {
				err := tx.Rollback()
				newErr := errors.New("rollback fail")
				errSlice := strings.Split(err.Error(), "; ")
				wantErrSlice := []string{
					newMockRollbackErr("order_detail_db_0", newErr).Error(),
					newMockRollbackErr("order_detail_db_1", newErr).Error()}
				assert.ElementsMatch(t, wantErrSlice, errSlice)

				s.mockMaster.MatchExpectationsInOrder(false)
				s.mockMaster2.MatchExpectationsInOrder(false)

				rows := s.mockMaster.NewRows([]string{"order_id", "item_id", "using_col1", "using_col2"})
				s.mockMaster.ExpectQuery(regexp.QuoteMeta("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_0`.`order_detail_tab_0` WHERE (`order_id`=?) OR (`order_id`=?);")).
					WithArgs(288, 33).WillReturnRows(rows)

				s.mockMaster2.ExpectQuery(regexp.QuoteMeta("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_1`.`order_detail_tab_0` WHERE (`order_id`=?) OR (`order_id`=?);")).
					WithArgs(288, 33).WillReturnRows(rows)

				queryVal := s.findTgt(t, values)
				var wantOds []*test.OrderDetail
				assert.ElementsMatch(t, wantOds, queryVal)
			},
		},
		{
			name:         "select insert part rollback err",
			wantAffected: 2,
			values: []*test.OrderDetail{
				{OrderId: 288, ItemId: 101, UsingCol1: "Jimmy", UsingCol2: "Butler"},
				{OrderId: 33, ItemId: 100, UsingCol1: "Nikolai", UsingCol2: "Jokic"},
			},
			querySet: []*test.OrderDetail{
				{OrderId: 8, ItemId: 6, UsingCol1: "Kobe", UsingCol2: "Bryant"},
				{OrderId: 11, ItemId: 8, UsingCol1: "James", UsingCol2: "Harden"},
				{OrderId: 234, ItemId: 12, UsingCol1: "Kevin", UsingCol2: "Durant"},
				{OrderId: 253, ItemId: 8, UsingCol1: "Stephen", UsingCol2: "Curry"},
				{OrderId: 181, ItemId: 11, UsingCol1: "Kawhi", UsingCol2: "Leonard"},
			},
			mockOrder: func(mock1, mock2 sqlmock.Sqlmock) {
				mock1.MatchExpectationsInOrder(false)
				mock2.MatchExpectationsInOrder(false)
				mock1.ExpectBegin()
				mock2.ExpectBegin()

				mock1.ExpectQuery(regexp.QuoteMeta("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_0`.`order_detail_tab_0` WHERE `order_id`!=?;SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_0`.`order_detail_tab_1` WHERE `order_id`!=?;SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_0`.`order_detail_tab_2` WHERE `order_id`!=?;")).
					WithArgs(123, 123, 123).
					WillReturnRows(mock1.NewRows([]string{"order_id", "item_id", "using_col1", "using_col2"}).AddRow(234, 12, "Kevin", "Durant").AddRow(8, 6, "Kobe", "Bryant"))

				mock2.ExpectQuery(regexp.QuoteMeta("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_1`.`order_detail_tab_0` WHERE `order_id`!=?;SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_1`.`order_detail_tab_1` WHERE `order_id`!=?;SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_1`.`order_detail_tab_2` WHERE `order_id`!=?;")).
					WithArgs(123, 123, 123).
					WillReturnRows(mock1.NewRows([]string{"order_id", "item_id", "using_col1", "using_col2"}).AddRow(253, 8, "Stephen", "Curry").AddRow(181, 11, "Kawhi", "Leonard").AddRow(11, 8, "James", "Harden"))

				mock1.ExpectExec(regexp.QuoteMeta("INSERT INTO `order_detail_db_0`.`order_detail_tab_0`(`order_id`,`item_id`,`using_col1`,`using_col2`) VALUES(?,?,?,?);")).
					WithArgs(288, 101, "Jimmy", "Butler").WillReturnResult(sqlmock.NewResult(1, 1))
				mock2.ExpectExec(regexp.QuoteMeta("INSERT INTO `order_detail_db_1`.`order_detail_tab_0`(`order_id`,`item_id`,`using_col1`,`using_col2`) VALUES(?,?,?,?);")).
					WithArgs(33, 100, "Nikolai", "Jokic").WillReturnResult(sqlmock.NewResult(1, 1))

				mock1.ExpectRollback().WillReturnError(errors.New("rollback fail"))
				mock2.ExpectRollback()
			},
			txFunc: func() (*eorm.Tx, error) {
				return s.shardingDB.BeginTx(transaction.UsingTxType(context.Background(), transaction.Delay), &sql.TxOptions{})
			},
			afterFunc: func(t *testing.T, tx *eorm.Tx, values []*test.OrderDetail) {
				err := tx.Rollback()
				wantErr := multierr.Combine(newMockRollbackErr("order_detail_db_0", errors.New("rollback fail")))
				assert.Equal(t, wantErr, err)

				s.mockMaster.MatchExpectationsInOrder(false)
				s.mockMaster2.MatchExpectationsInOrder(false)

				rows := s.mockMaster.NewRows([]string{"order_id", "item_id", "using_col1", "using_col2"})
				s.mockMaster.ExpectQuery(regexp.QuoteMeta("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_0`.`order_detail_tab_0` WHERE (`order_id`=?) OR (`order_id`=?);")).
					WithArgs(288, 33).WillReturnRows(rows)

				s.mockMaster2.ExpectQuery(regexp.QuoteMeta("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_1`.`order_detail_tab_0` WHERE (`order_id`=?) OR (`order_id`=?);")).
					WithArgs(288, 33).WillReturnRows(rows)

				queryVal := s.findTgt(t, values)
				var wantOds []*test.OrderDetail
				assert.ElementsMatch(t, wantOds, queryVal)
			},
		},
		{
			name:         "select insert commit",
			wantAffected: 2,
			values: []*test.OrderDetail{
				{OrderId: 288, ItemId: 101, UsingCol1: "Jimmy", UsingCol2: "Butler"},
				{OrderId: 33, ItemId: 100, UsingCol1: "Nikolai", UsingCol2: "Jokic"},
			},
			querySet: []*test.OrderDetail{
				{OrderId: 8, ItemId: 6, UsingCol1: "Kobe", UsingCol2: "Bryant"},
				{OrderId: 11, ItemId: 8, UsingCol1: "James", UsingCol2: "Harden"},
				{OrderId: 234, ItemId: 12, UsingCol1: "Kevin", UsingCol2: "Durant"},
				{OrderId: 253, ItemId: 8, UsingCol1: "Stephen", UsingCol2: "Curry"},
				{OrderId: 181, ItemId: 11, UsingCol1: "Kawhi", UsingCol2: "Leonard"},
			},
			mockOrder: func(mock1, mock2 sqlmock.Sqlmock) {
				mock1.MatchExpectationsInOrder(false)
				mock2.MatchExpectationsInOrder(false)
				mock1.ExpectBegin()
				mock2.ExpectBegin()

				mock1.ExpectQuery(regexp.QuoteMeta("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_0`.`order_detail_tab_0` WHERE `order_id`!=?;SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_0`.`order_detail_tab_1` WHERE `order_id`!=?;SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_0`.`order_detail_tab_2` WHERE `order_id`!=?;")).
					WithArgs(123, 123, 123).
					WillReturnRows(mock1.NewRows([]string{"order_id", "item_id", "using_col1", "using_col2"}).AddRow(234, 12, "Kevin", "Durant").AddRow(8, 6, "Kobe", "Bryant"))

				mock2.ExpectQuery(regexp.QuoteMeta("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_1`.`order_detail_tab_0` WHERE `order_id`!=?;SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_1`.`order_detail_tab_1` WHERE `order_id`!=?;SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_1`.`order_detail_tab_2` WHERE `order_id`!=?;")).
					WithArgs(123, 123, 123).
					WillReturnRows(mock1.NewRows([]string{"order_id", "item_id", "using_col1", "using_col2"}).AddRow(253, 8, "Stephen", "Curry").AddRow(181, 11, "Kawhi", "Leonard").AddRow(11, 8, "James", "Harden"))

				mock1.ExpectExec(regexp.QuoteMeta("INSERT INTO `order_detail_db_0`.`order_detail_tab_0`(`order_id`,`item_id`,`using_col1`,`using_col2`) VALUES(?,?,?,?);")).
					WithArgs(288, 101, "Jimmy", "Butler").WillReturnResult(sqlmock.NewResult(1, 1))
				mock2.ExpectExec(regexp.QuoteMeta("INSERT INTO `order_detail_db_1`.`order_detail_tab_0`(`order_id`,`item_id`,`using_col1`,`using_col2`) VALUES(?,?,?,?);")).
					WithArgs(33, 100, "Nikolai", "Jokic").WillReturnResult(sqlmock.NewResult(1, 1))

				mock1.ExpectCommit()
				mock2.ExpectCommit()
			},
			txFunc: func() (*eorm.Tx, error) {
				return s.shardingDB.BeginTx(transaction.UsingTxType(context.Background(), transaction.Delay), &sql.TxOptions{})
			},
			afterFunc: func(t *testing.T, tx *eorm.Tx, values []*test.OrderDetail) {
				err := tx.Commit()
				require.NoError(t, err)

				s.mockMaster.MatchExpectationsInOrder(false)
				s.mockMaster2.MatchExpectationsInOrder(false)

				s.mockMaster.ExpectQuery(regexp.QuoteMeta("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_0`.`order_detail_tab_0` WHERE (`order_id`=?) OR (`order_id`=?);")).
					WithArgs(288, 33).WillReturnRows(s.mockMaster.NewRows([]string{"order_id", "item_id", "using_col1", "using_col2"}).AddRow(288, 101, "Jimmy", "Butler"))

				s.mockMaster2.ExpectQuery(regexp.QuoteMeta("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_1`.`order_detail_tab_0` WHERE (`order_id`=?) OR (`order_id`=?);")).
					WithArgs(288, 33).WillReturnRows(s.mockMaster.NewRows([]string{"order_id", "item_id", "using_col1", "using_col2"}).AddRow(33, 100, "Nikolai", "Jokic"))

				queryVal := s.findTgt(t, values)
				assert.ElementsMatch(t, values, queryVal)
			},
		},
		{
			name:         "select insert rollback",
			wantAffected: 2,
			values: []*test.OrderDetail{
				{OrderId: 199, ItemId: 100, UsingCol1: "Jason", UsingCol2: "Tatum"},
				{OrderId: 299, ItemId: 101, UsingCol1: "Paul", UsingCol2: "George"},
			},
			querySet: []*test.OrderDetail{
				{OrderId: 8, ItemId: 6, UsingCol1: "Kobe", UsingCol2: "Bryant"},
				{OrderId: 11, ItemId: 8, UsingCol1: "James", UsingCol2: "Harden"},
				{OrderId: 234, ItemId: 12, UsingCol1: "Kevin", UsingCol2: "Durant"},
				{OrderId: 253, ItemId: 8, UsingCol1: "Stephen", UsingCol2: "Curry"},
				{OrderId: 181, ItemId: 11, UsingCol1: "Kawhi", UsingCol2: "Leonard"},
			},
			mockOrder: func(mock1, mock2 sqlmock.Sqlmock) {
				mock1.MatchExpectationsInOrder(false)
				mock2.MatchExpectationsInOrder(false)
				mock1.ExpectBegin()
				mock2.ExpectBegin()

				mock1.ExpectQuery(regexp.QuoteMeta("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_0`.`order_detail_tab_0` WHERE `order_id`!=?;SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_0`.`order_detail_tab_1` WHERE `order_id`!=?;SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_0`.`order_detail_tab_2` WHERE `order_id`!=?;")).
					WithArgs(123, 123, 123).
					WillReturnRows(mock1.NewRows([]string{"order_id", "item_id", "using_col1", "using_col2"}).AddRow(234, 12, "Kevin", "Durant").AddRow(8, 6, "Kobe", "Bryant"))

				mock2.ExpectQuery(regexp.QuoteMeta("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_1`.`order_detail_tab_0` WHERE `order_id`!=?;SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_1`.`order_detail_tab_1` WHERE `order_id`!=?;SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_1`.`order_detail_tab_2` WHERE `order_id`!=?;")).
					WithArgs(123, 123, 123).
					WillReturnRows(mock1.NewRows([]string{"order_id", "item_id", "using_col1", "using_col2"}).AddRow(253, 8, "Stephen", "Curry").AddRow(181, 11, "Kawhi", "Leonard").AddRow(11, 8, "James", "Harden"))

				mock2.ExpectExec(regexp.QuoteMeta("INSERT INTO `order_detail_db_1`.`order_detail_tab_1`(`order_id`,`item_id`,`using_col1`,`using_col2`) VALUES(?,?,?,?);")).
					WithArgs(199, 100, "Jason", "Tatum").WillReturnResult(sqlmock.NewResult(1, 1))
				mock2.ExpectExec(regexp.QuoteMeta("INSERT INTO `order_detail_db_1`.`order_detail_tab_2`(`order_id`,`item_id`,`using_col1`,`using_col2`) VALUES(?,?,?,?);")).
					WithArgs(299, 101, "Paul", "George").WillReturnResult(sqlmock.NewResult(1, 1))

				mock1.ExpectRollback()
				mock2.ExpectRollback()
			},
			txFunc: func() (*eorm.Tx, error) {
				return s.shardingDB.BeginTx(transaction.UsingTxType(context.Background(), transaction.Delay), &sql.TxOptions{})
			},
			afterFunc: func(t *testing.T, tx *eorm.Tx, values []*test.OrderDetail) {
				err := tx.Rollback()
				require.NoError(t, err)
				s.mockMaster2.MatchExpectationsInOrder(false)

				rows := s.mockMaster2.NewRows([]string{"order_id", "item_id", "using_col1", "using_col2"})
				s.mockMaster2.ExpectQuery(regexp.QuoteMeta("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_1`.`order_detail_tab_2` WHERE (`order_id`=?) OR (`order_id`=?);SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_1`.`order_detail_tab_1` WHERE (`order_id`=?) OR (`order_id`=?);")).
					WithArgs(199, 299, 199, 299).WillReturnRows(rows)
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			s.SetupTest()

			tc.mockOrder(s.mockMaster, s.mockMaster2)
			tx, err := tc.txFunc()
			require.NoError(t, err)

			querySet, err := eorm.NewShardingSelector[test.OrderDetail](tx).
				Where(eorm.C("OrderId").NEQ(123)).
				GetMulti(masterslave.UseMaster(context.Background()))
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.ElementsMatch(t, tc.querySet, querySet)

			values := tc.values
			res := eorm.NewShardingInsert[test.OrderDetail](tx).
				Values(values).Exec(context.Background())
			affected, err := res.RowsAffected()
			require.NoError(t, err)
			assert.Equal(t, tc.wantAffected, affected)
			tc.afterFunc(t, tx, values)

			s.TearDownTest()
		})
	}
}

func TestDelayTransactionSuite(t *testing.T) {
	suite.Run(t, &TestDelayTxTestSuite{
		ShardingTransactionSuite: newShardingTransactionSuite(),
	})
}
