/*
* Copyright (c) 2021 yedf. All rights reserved.
* Use of this source code is governed by a BSD-style
* license that can be found in the LICENSE file.
 */

package aerospikedb

//import (
//	"errors"
//	"fmt"
//	bolt "go.etcd.io/bbolt"
//	"time"
//
//	as "github.com/aerospike/aerospike-client-go/v5"
//	"github.com/dtm-labs/dtm/dtmcli"
//	"github.com/dtm-labs/dtm/dtmcli/dtmimp"
//	"github.com/dtm-labs/dtm/dtmcli/logger"
//	"github.com/dtm-labs/dtm/dtmsvr/storage"
//	"github.com/dtm-labs/dtm/dtmutil"
//	"github.com/rs/xid"
//)
//
////// Store implements storage.Store, and storage with boltdb
////type Store struct {
////	aerospikeDb   *as.Client
////	endpoints     *[]Node
////	dataExpire    int64
////	retryInterval int64
////}
//
//// NewStore will return the boltdb implement
//// TODO: change to options
//func NewStore(dataExpire int64, retryInterval int64) *Store {
//	s := &Store{
//		dataExpire:    dataExpire,
//		retryInterval: retryInterval,
//	}
//
//	policy := as.NewWritePolicy(0, 0)
//	policy.CommitLevel = as.COMMIT_ALL
//	policy.TotalTimeout = 50 * time.Millisecond
//
//	client, err := GetConnection(s)
//	dtmimp.E2P(err)
//
//	// NOTE: we must ensure all buckets is exists before we use it
//	err = initializeBins(db)
//	dtmimp.E2P(err)
//
//	// TODO:
//	//   1. refactor this code
//	//   2. make cleanup run period, to avoid the file growup when server long-running
//	err = cleanupExpiredData(
//		time.Duration(dataExpire)*time.Second,
//		db,
//	)
//	dtmimp.E2P(err)
//
//	s.aerospikeDb = client
//	s.boltDb = db
//	return s
//}
//
//const GLOBALXID_STATUS = "gxid_status"
//const RECORD_KEY = "record_key"
//const TIMEOUT = "timeout"
//
//type TxIdStatus int32
//
//const (
//	Unused      TxIdStatus = -1
//	Initialized            = 0
//	Locked                 = 1
//	Unlocked               = 2
//	Complete               = 3
//	Unknown                = 4
//)
//
//var TransactionManagerNamespace string = "test"
//var TransactionManagerSet string = "globaltx_astm"
//var TransactionSet string = "tx_astm"
//
//func initializeBins(db *as.Client, policy *as.WritePolicy) error {
//	gtxKey, err := CreateGlobalTransaction(db, policy, 550, Unused)
//	if err != nil {
//		return err
//	}
//
//	err = CreateSubTransaction(db, policy, 550, gtxKey, Unused)
//	if err != nil {
//		return err
//	}
//	return nil
//}
//
//func CreateGlobalTransaction(db *as.Client, policy *as.WritePolicy, timeOut int32, status TxIdStatus) (*as.Key, error) {
//	// Records for Global Transactions consists of
//	// The Global Transaction Records consists of TrnxId:timestamp, status(locked/unlocked), TTL
//	// The Key is trnxId:timestamp
//	// Create the GlobalTransaction Set
//	// With an initial dumb transaction to know that is where it starts
//
//	//Initial NewKey
//	guid := xid.New()
//	keyValue, timeout := EncodeKey(guid, 550)
//
//	// This is the Global TXId Set
//	key, err := as.NewKey(TransactionManagerNamespace, TransactionManagerSet, keyValue)
//	dtmimp.E2P(err)
//
//	var binMap as.BinMap
//	binMap[GLOBALXID_STATUS] = status
//	binMap[TIMEOUT] = timeout
//	db.Put(policy, key, binMap)
//	return key, nil
//}
//
//func CreateSubTransaction(db *as.Client, policy *as.WritePolicy, timeOut int32, gtxKey *as.Key, status TxIdStatus) error {
//	// Records for Global Transactions consists of
//	// The Global Transaction Records consists of TrnxId:timestamp, status(locked/unlocked), TTL
//	// The Key is trnxId:timestamp
//	// Create the GlobalTransaction Set
//	// With an initial dumb transaction to know that is where it starts
//
//	//Initial NewKey
//	guid := xid.New()
//	keyValue, timeout := EncodeKey(guid, 110)
//
//	// This is the Global TXId Set
//	key, err := as.NewKey(TransactionManagerNamespace, TransactionSet, keyValue)
//	dtmimp.E2P(err)
//
//	var binMap as.BinMap
//	binMap[GLOBALXID_STATUS] = status
//	binMap[RECORD_KEY] = gtxKey
//	binMap[TIMEOUT] = timeout
//	binMap[TransactionManagerSet] = gtxKey
//
//	db.Put(policy, key, binMap)
//
//	return nil
//}
//
//func cleanupExpiredTransactionIds(db *as.Client, setName string, timeout time.Duration) {
//	// Go get all global transactins that have expired and client them up
//	policy := as.NewQueryPolicy()
//	bins := []string{GLOBALXID_STATUS, TIMEOUT}
//
//	statement := &as.Statement{
//		Namespace: TransactionManagerNamespace,
//		SetName:   setName,
//		IndexName: "",
//		BinNames:  bins,
//		Filter:    nil,
//		TaskId:    0,
//	}
//
//	currentTime := time.Now()
//	timeOut := currentTime.Local().Add(-1 * timeout)
//
//	statement.SetFilter((as.NewRangeFilter(TIMEOUT, int64(timeOut.UnixNano()), int64(currentTime.UnixNano()))))
//	recordset, err := db.Query(policy, statement)
//
//	if err != nil {
//		dtmimp.E2P(err)
//	}
//
//	for res := range recordset.Results() {
//		if res.Err != nil {
//			// handle error here
//		} else {
//			logger.Infof("deleting key %v with timeout in nanosecond of %d and status of %d", res.Record.Key, res.Record.Bins[TIMEOUT], res.Record.Bins[GLOBALXID_STATUS])
//			db.Delete(nil, res.Record.Key)
//		}
//	}
//
//	logger.Infof("clear all expired transaction for set %s", setName)
//}
//
//func cleanupGlobalWithGids(db *as.Client) {
//	cleanupExpiredTransactionIds(db, TransactionManagerSet, time.Duration((500 * time.Millisecond)))
//}
//
//func cleanupExpiredSubTransactions(db *as.Client) {
//	cleanupExpiredTransactionIds(db, TransactionSet, time.Duration((500 * time.Millisecond)))
//}
//
//func cleanupCompletedUnknownTransactions(db *as.Client) {
//	// Go get all current transactions that are either complete/unknown and delete those
//	policy := as.NewQueryPolicy()
//	bins := []string{GLOBALXID_STATUS}
//
//	statement := &as.Statement{
//		Namespace: TransactionManagerNamespace,
//		SetName:   TransactionManagerSet,
//		IndexName: "",
//		BinNames:  bins,
//		Filter:    nil,
//		TaskId:    0,
//	}
//
//	statement.SetFilter((as.NewRangeFilter(GLOBALXID_STATUS, int64(Complete), int64(Unknown))))
//	recordset, err := db.Query(policy, statement)
//
//	if err != nil {
//		dtmimp.E2P(err)
//	}
//
//	for res := range recordset.Results() {
//		if res.Err != nil {
//			// handle error here
//		} else {
//			logger.Infof("deleting key %v with status of %d", res.Record.Key, res.Record.Bins[GLOBALXID_STATUS])
//			db.Delete(nil, res.Record.Key)
//		}
//	}
//
//	logger.Infof("removed all completed or unknow transactions")
//}
//
//// Ping execs ping cmd to aerospikedb
//func (s *Store) Ping() error {
//	if s.aerospikeDb.IsConnected() == true {
//		return nil
//	}
//
//	return errors.New("not connected to aerospike cluster")
//}
//
//// PopulateData populates data to Aerospike
//func (s *Store) PopulateData(skipDrop bool) {
//	if !skipDrop {
//		cleanupCompletedUnknownTransactions(s.aerospikeDb)
//		cleanupGlobalWithGids(s.aerospikeDb)
//		cleanupExpiredSubTransactions(s.aerospikeDb)
//	}
//}
//
//// FindTransGlobalStore finds GlobalTrans data by gid
//func (s *Store) FindTransGlobalStore(gid string) (trans *storage.TransGlobalStore) {
//	err := s.boltDb.View(func(t *bolt.Tx) error {
//		trans = tGetGlobal(t, gid)
//		return nil
//	})
//	dtmimp.E2P(err)
//	return
//}
//
//// ScanTransGlobalStores lists GlobalTrans data
//func (s *Store) ScanTransGlobalStores(position *string, limit int64) []storage.TransGlobalStore {
//	globals := []storage.TransGlobalStore{}
//	err := s.boltDb.View(func(t *bolt.Tx) error {
//		cursor := t.Bucket(bucketGlobal).Cursor()
//		for k, v := cursor.Seek([]byte(*position)); k != nil; k, v = cursor.Next() {
//			if string(k) == *position {
//				continue
//			}
//			g := storage.TransGlobalStore{}
//			dtmimp.MustUnmarshal(v, &g)
//			globals = append(globals, g)
//			if len(globals) == int(limit) {
//				break
//			}
//		}
//		return nil
//	})
//	dtmimp.E2P(err)
//	if len(globals) < int(limit) {
//		*position = ""
//	} else {
//		*position = globals[len(globals)-1].Gid
//	}
//	return globals
//}
//
//// FindBranches finds Branch data by gid
//func (s *Store) FindBranches(gid string) []storage.TransBranchStore {
//	var branches []storage.TransBranchStore
//	err := s.boltDb.View(func(t *bolt.Tx) error {
//		branches = tGetBranches(t, gid)
//		return nil
//	})
//	dtmimp.E2P(err)
//	return branches
//}
//
//// UpdateBranches update branches info
//func (s *Store) UpdateBranches(branches []storage.TransBranchStore, updates []string) (int, error) {
//	return 0, nil // not implemented
//}
//
//// LockGlobalSaveBranches creates branches
//func (s *Store) LockGlobalSaveBranches(gid string, status string, branches []storage.TransBranchStore, branchStart int) {
//	err := s.boltDb.Update(func(t *bolt.Tx) error {
//		g := tGetGlobal(t, gid)
//		if g == nil {
//			return storage.ErrNotFound
//		}
//		if g.Status != status {
//			return storage.ErrNotFound
//		}
//		tPutBranches(t, branches, int64(branchStart))
//		return nil
//	})
//	dtmimp.E2P(err)
//}
//
//// MaySaveNewTrans creates a new trans
//func (s *Store) MaySaveNewTrans(global *storage.TransGlobalStore, branches []storage.TransBranchStore) error {
//	return s.boltDb.Update(func(t *bolt.Tx) error {
//		g := tGetGlobal(t, global.Gid)
//		if g != nil {
//			return storage.ErrUniqueConflict
//		}
//		tPutGlobal(t, global)
//		tPutIndex(t, global.NextCronTime.Unix(), global.Gid)
//		tPutBranches(t, branches, 0)
//		return nil
//	})
//}
//
//// ChangeGlobalStatus changes global trans status
//func (s *Store) ChangeGlobalStatus(global *storage.TransGlobalStore, newStatus string, updates []string, finished bool) {
//	old := global.Status
//	global.Status = newStatus
//	err := s.boltDb.Update(func(t *bolt.Tx) error {
//		g := tGetGlobal(t, global.Gid)
//		if g == nil || g.Status != old {
//			return storage.ErrNotFound
//		}
//		if finished {
//			tDelIndex(t, g.NextCronTime.Unix(), g.Gid)
//		}
//		tPutGlobal(t, global)
//		return nil
//	})
//	dtmimp.E2P(err)
//}
//
//// TouchCronTime updates cronTime
//func (s *Store) TouchCronTime(global *storage.TransGlobalStore, nextCronInterval int64, nextCronTime *time.Time) {
//	oldUnix := global.NextCronTime.Unix()
//	global.UpdateTime = dtmutil.GetNextTime(0)
//	global.NextCronTime = nextCronTime
//	global.NextCronInterval = nextCronInterval
//	err := s.boltDb.Update(func(t *bolt.Tx) error {
//		g := tGetGlobal(t, global.Gid)
//		if g == nil || g.Gid != global.Gid {
//			return storage.ErrNotFound
//		}
//		tDelIndex(t, oldUnix, global.Gid)
//		tPutGlobal(t, global)
//		tPutIndex(t, global.NextCronTime.Unix(), global.Gid)
//		return nil
//	})
//	dtmimp.E2P(err)
//}
//
//// LockOneGlobalTrans finds GlobalTrans
//func (s *Store) LockOneGlobalTrans(expireIn time.Duration) *storage.TransGlobalStore {
//	var trans *storage.TransGlobalStore
//	min := fmt.Sprintf("%d", time.Now().Add(expireIn).Unix())
//	next := time.Now().Add(time.Duration(s.retryInterval) * time.Second)
//	err := s.boltDb.Update(func(t *bolt.Tx) error {
//		cursor := t.Bucket(bucketIndex).Cursor()
//		toDelete := [][]byte{}
//		for trans == nil || trans.Status == dtmcli.StatusSucceed || trans.Status == dtmcli.StatusFailed {
//			k, v := cursor.First()
//			if k == nil || string(k) > min {
//				return storage.ErrNotFound
//			}
//			trans = tGetGlobal(t, string(v))
//			toDelete = append(toDelete, k)
//		}
//		for _, k := range toDelete {
//			err := t.Bucket(bucketIndex).Delete(k)
//			dtmimp.E2P(err)
//		}
//		trans.NextCronTime = &next
//		tPutGlobal(t, trans)
//		tPutIndex(t, next.Unix(), trans.Gid)
//		return nil
//	})
//	if err == storage.ErrNotFound {
//		return nil
//	}
//	dtmimp.E2P(err)
//	return trans
//}
//
//// ResetCronTime rest nextCronTime
//// Prevent multiple backoff from causing NextCronTime to be too long
//func (s *Store) ResetCronTime(timeout time.Duration, limit int64) (succeedCount int64, hasRemaining bool, err error) {
//	next := time.Now()
//	var trans *storage.TransGlobalStore
//	min := fmt.Sprintf("%d", time.Now().Add(timeout).Unix())
//	err = s.boltDb.Update(func(t *bolt.Tx) error {
//		cursor := t.Bucket(bucketIndex).Cursor()
//		succeedCount = 0
//		for k, v := cursor.Seek([]byte(min)); k != nil && succeedCount <= limit; k, v = cursor.Next() {
//			if succeedCount == limit {
//				hasRemaining = true
//				break
//			}
//
//			trans = tGetGlobal(t, string(v))
//			err := t.Bucket(bucketIndex).Delete(k)
//			dtmimp.E2P(err)
//
//			trans.NextCronTime = &next
//			tPutGlobal(t, trans)
//			tPutIndex(t, next.Unix(), trans.Gid)
//			succeedCount++
//		}
//		return nil
//	})
//	return
//}
