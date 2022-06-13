/*
 * Copyright (c) 2022 yedf. All rights reserved.
 * Use of this source code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */

package aerospikedb

import (
	"context"
	"errors"
	as "github.com/aerospike/aerospike-client-go/v5"
	"github.com/dtm-labs/dtm/dtmsvr/storage/aerospikedb/pooler"
	"time"

	"github.com/dtm-labs/dtm/dtmcli/dtmimp"
	"github.com/dtm-labs/dtm/dtmcli/logger"
	"github.com/dtm-labs/dtm/dtmsvr/config"
	"github.com/dtm-labs/dtm/dtmsvr/storage"
)

// TODO: optimize this, it's very strange to use pointer to dtmutil.Config
var conf = &config.Config

// TODO: optimize this, all function should have context as first parameter
var ctx = context.Background()

// Store is the storage with redis, all transaction information will bachend with redis
type Store struct {
	//aerospikeDb   *as.Client
	//endpoints     *[]Node
}

var connectionPools *pooler.ASConnectionPool

func InitializeAerospikeStore(store config.Store) {
	cp, err := pooler.InitializeConnectionPool(store)
	dtmimp.E2P(err)
	logger.Infof("Connection Pool initialized with connection depth: %d", cp.PoolDepth())
	connectionPools = cp
}

// Ping execs ping cmd to redis
func (s *Store) Ping() error {

	if aerospikeGet().IsConnected() == true {
		return nil
	}

	return errors.New("not connected to aerospike cluster")
}

// To Do
// this neds to be implemented for aerospike
// Basically we want to delete all the sets that are created by the transaction manager

// PopulateData populates data to redis
func (s *Store) PopulateData(skipDrop bool) {

	if !skipDrop {
		DropTableTransGlobal()
		DropTableTransBranchOp()
		//_, err := redisGet().FlushAll(ctx).Result()
		//logger.Infof("call redis flushall. result: %v", err)
		//dtmimp.PanicIf(err != nil, err)
	}
	CreateTransGlobalSet()
	CreateTransBranchOpSet()

}

// FindTransGlobalStore finds GlobalTrans data by gid
func (s *Store) FindTransGlobalStore(gid string) *storage.TransGlobalStore {
	logger.Debugf("calling FindTransGlobalStore: %s", gid)

	result := GetTransGlobal(gid)
	if result == nil {
		return nil
	}
	trans := &storage.TransGlobalStore{}
	dtmimp.MustUnmarshalString(*result, trans)
	return trans
}

// ScanTransGlobalStores lists GlobalTrans data
func (s *Store) ScanTransGlobalStores(position *string, limit int64) []storage.TransGlobalStore {
	logger.Debugf("calling ScanTransGlobalStores: positiion: %s, limit: %d", *position, limit)
	results, pos := ScanTransGlobalTable(position, limit)

	*position = *pos
	logger.Debugf("ScanTransGlobalStores: position value, %s", position)
	globals := []storage.TransGlobalStore{}

	if results == nil {
		return globals
	}

	for _, v := range *results {
		global := storage.TransGlobalStore{}
		dtmimp.MustUnmarshalString(v, &global)
		globals = append(globals, global)
	}
	return globals
}

// FindBranches finds Branch data by gid
func (s *Store) FindBranches(gid string) []storage.TransBranchStore {
	logger.Debugf("calling FindBranches: %s", gid)

	branches := []storage.TransBranchStore{}

	results := GetBranchs(gid)
	if results == nil {
		return branches
	}

	//branches := make([]storage.TransBranchStore, len(*results))
	for _, v := range *results {
		b := storage.TransBranchStore{}
		dtmimp.MustUnmarshalString(v, &b)
		branches = append(branches, b)
	}

	return branches
}

// UpdateBranches updates branches info
func (s *Store) UpdateBranches(branches []storage.TransBranchStore, updates []string) (int, error) {
	logger.Infof("Unimplemented, here is the data branches: %v with updates: %v", branches, updates)
	return 0, nil // not implemented
}

// MaySaveNewTrans creates a new trans
func (s *Store) MaySaveNewTrans(global *storage.TransGlobalStore, branches []storage.TransBranchStore) error {

	exist := CheckTransGlobalTableForGIDExists(global.Gid)
	logger.Debugf("MaySaveNewTrans: checking if gid(%s) exists(%t)", global.Gid, exist)
	if exist == true {
		return errors.New("UNIQUE_CONFLICT")
	}

	var branchGidList []string
	for _, v := range branches {
		logger.Debugf("MaySaveNewTrans: saving global trans for branch gid(%s)", v.Gid)
		branchGidList = append(branchGidList, v.Gid)
	}

	err := NewTransGlobal(global, &branchGidList)
	if err != nil {
		return err
	}

	err = NewTransBranchOpSet(branches)
	if err != nil {
		return err
	}

	return nil
}

// LockGlobalSaveBranches creates branches
func (s *Store) LockGlobalSaveBranches(gid string, status string, branches []storage.TransBranchStore, branchStart int) {
	err := UpdateBranchsWithGIDStatus(gid, status, branches)
	dtmimp.E2P(err)
}

// ChangeGlobalStatus changes global trans status
func (s *Store) ChangeGlobalStatus(global *storage.TransGlobalStore, newStatus string, updates []string, finished bool) {
	logger.Debugf("ChangeGlobalStatus: trans to change, %v", *global)
	UpdateGlobalStatus(global, newStatus, updates, finished)
}

// LockOneGlobalTrans finds GlobalTrans
func (s *Store) LockOneGlobalTrans(expireIn time.Duration) *storage.TransGlobalStore {
	logger.Debugf("LockOneGlobalTrans: with expireIn %v", expireIn)
	result := LockOneGlobalTransTrans(expireIn)
	if result == nil {
		return nil
	}
	return result
}

// ResetCronTime rest nextCronTime
// Prevent multiple backoff from causing NextCronTime to be too long
func (s *Store) ResetCronTime(timeout time.Duration, limit int64) (succeedCount int64, hasRemaining bool, err error) {
	return ResetCronTimeGlobalTran(timeout, limit)
}

// TouchCronTime updates cronTime
func (s *Store) TouchCronTime(global *storage.TransGlobalStore, nextCronInterval int64, nextCronTime *time.Time) {

	TouchCronTimeGlobalTran(global, nextCronInterval, nextCronTime)
}

func aerospikeGet() *as.Client {
	asConnIntf, err := connectionPools.Get()
	if err != nil {
		logger.Errorf(err.Error())
		return nil
	}
	return asConnIntf.(*as.Client)
}
