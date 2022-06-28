package aerospikedb

import (
	"errors"
	"fmt"
	as "github.com/aerospike/aerospike-client-go/v5"
	"github.com/dtm-labs/dtm/dtmcli/dtmimp"
	"github.com/dtm-labs/dtm/dtmcli/logger"
	"github.com/dtm-labs/dtm/dtmsvr/storage"
	"github.com/dtm-labs/dtm/dtmutil"
	"github.com/lithammer/shortuuid/v3"
	"github.com/rs/xid"
	"time"
)

const SCHEMA = "test"

var TransactionManagerNamespace = "test"
var TransactionGlobal = "trans_global"
var TransactionBranchOp = "trans_branch_op"

type TEXT string
type BYTEA string

func getTransGlobalTableBins() *[]string {
	bins := []string{
		"xid",
		"gid",
		"trans_type",
		"status",
		"query_prepared",
		"protocol",
		"create_time",
		"update_time",
		"finish_time",
		"rollback_time",
		"options",
		"custom_data",
		"nxt_cron_intrvl",
		"next_cron_time",
		"ext_data",
		"owner",
		"branches",
	}
	return &bins
}

func DropTableTransGlobal() {
	//drop table IF EXISTS dtm.trans_global;
	client := aerospikeGet()
	defer connectionPools.Put(client)

	writePolicy := &as.WritePolicy{
		RecordExistsAction: as.REPLACE,
		DurableDelete:      true,
		CommitLevel:        as.COMMIT_MASTER,
	}
	now := time.Now()
	err := client.Truncate(writePolicy, SCHEMA, TransactionGlobal, &now)
	dtmimp.E2P(err)

	err = client.DropIndex(writePolicy, SCHEMA, TransactionGlobal, "TXM_XID")
	dtmimp.E2P(err)

	err = client.DropIndex(writePolicy, SCHEMA, TransactionGlobal, "TXM_GID")
	dtmimp.E2P(err)

	err = client.DropIndex(writePolicy, SCHEMA, TransactionGlobal, "TXM_OWNER")
	dtmimp.E2P(err)

	err = client.DropIndex(writePolicy, SCHEMA, TransactionGlobal, "st_nxt_ctime")
	dtmimp.E2P(err)

}

func CreateTransGlobalSet() {
	//CREATE TABLE if not EXISTS dtm.trans_global (
	//	id bigint NOT NULL DEFAULT NEXTVAL ('dtm.trans_global_seq'),
	//	gid varchar(128) NOT NULL,
	//	trans_type varchar(45) not null,
	//	status varchar(45) NOT NULL,
	//	query_prepared varchar(1024) NOT NULL,
	//	protocol varchar(45) not null,
	//	create_time timestamp(0) with time zone DEFAULT NULL,
	//	update_time timestamp(0) with time zone DEFAULT NULL,
	//	finish_time timestamp(0) with time zone DEFAULT NULL,
	//	rollback_time timestamp(0) with time zone DEFAULT NULL,
	//	options varchar(1024) DEFAULT '',
	//	custom_data varchar(256) DEFAULT '',
	//	nxt_cron_intrvl int default null,
	//next_cron_time timestamp(0) with time zone default null,
	//owner varchar(128) not null default '',
	//ext_data text,
	//PRIMARY KEY (id),
	//CONSTRAINT gid UNIQUE (gid)
	//);
	client := aerospikeGet()
	defer connectionPools.Put(client)

	var trans storage.TransGlobalStore

	txid := xid.New()
	key, err := as.NewKey(SCHEMA, TransactionGlobal, txid.Bytes())
	dtmimp.E2P(err)

	cdtStatusNextCronTime := map[string]interface{}{
		"status":         trans.Status,
		"next_cron_time": 0,
	}

	branches := []string{}

	bins := as.BinMap{
		"xid":             txid,
		"gid":             trans.Gid,
		"trans_type":      trans.TransType,
		"status":          trans.Status,
		"query_prepared":  trans.QueryPrepared,
		"protocol":        trans.Protocol,
		"create_time":     0,
		"update_time":     0,
		"finish_time":     0,
		"rollback_time":   0,
		"options":         trans.Options,
		"custom_data":     trans.CustomData,
		"next_cron_intvl": trans.NextCronInterval,
		"next_cron_time":  0,
		"owner":           trans.Owner,
		"branches":        branches,
		"st_nxt_ctime":    cdtStatusNextCronTime,
	}

	policy := as.NewWritePolicy(0, 0)
	policy.CommitLevel = as.COMMIT_ALL
	policy.TotalTimeout = 200 * time.Millisecond
	policy.RecordExistsAction = as.REPLACE

	err = client.Put(policy, key, bins)
	dtmimp.E2P(err)

	//create index if not EXISTS xid ;
	indexTaskXid, err := client.CreateIndex(policy, TransactionManagerNamespace, TransactionGlobal, "TXM_XID", "xid", as.STRING)
	dtmimp.E2P(err)
	IdxerrXid := <-indexTaskXid.OnComplete()
	dtmimp.E2P(IdxerrXid)

	//create index if not EXISTS gid on dtm.trans_global(gid);
	indexTask, err := client.CreateIndex(policy, TransactionManagerNamespace, TransactionGlobal, "TXM_GID", "gid", as.STRING)
	dtmimp.E2P(err)

	Idxerr := <-indexTask.OnComplete()
	dtmimp.E2P(Idxerr)

	//create index if not EXISTS owner on dtm.trans_global(owner);
	indexTask1, err := client.CreateIndex(policy, TransactionManagerNamespace, TransactionGlobal, "TXM_OWNER", "owner", as.STRING)
	dtmimp.E2P(err)

	Idxerr1 := <-indexTask1.OnComplete()
	dtmimp.E2P(Idxerr1)

	//create index if not EXISTS status_next_cron_time on dtm.trans_global (status, next_cron_time);
	indexTask2, err := client.CreateComplexIndex(policy, TransactionManagerNamespace, TransactionGlobal, "st_nxt_ctime", "st_nxt_ctime", as.STRING, as.ICT_MAPVALUES)
	Idxerr2 := <-indexTask2.OnComplete()
	dtmimp.E2P(Idxerr2)

	now := time.Now()
	errTruncate := client.Truncate(policy, SCHEMA, TransactionGlobal, &now)
	dtmimp.E2P(errTruncate)

}

func NewTransGlobal(global *storage.TransGlobalStore, branches *[]xid.ID) error {
	logger.Debugf("NewTransGlobal: with gid %s and %d branches", global.Gid, len(*branches))
	logger.Debugf("NewTransGlobal: create time %s", global.CreateTime)

	client := aerospikeGet()
	defer connectionPools.Put(client)

	txid := xid.New()
	key, err := as.NewKey(SCHEMA, TransactionGlobal, global.Gid)
	dtmimp.E2P(err)

	next_cron_time := global.NextCronTime.UnixNano()

	cdtStatusNextCronTime := map[string]interface{}{
		"status":         global.Status,
		"next_cron_time": next_cron_time,
	}

	if global.TransType == "" {
		logger.Debugf("ERROR ERROR ERROR trans_type is empty")
		//dtmimp.E2P(errors.New("ERROR ERROR ERROR trans_type is empty"))
	}

	var createTime int64
	if global.CreateTime != nil {
		createTime = global.CreateTime.UnixNano()
	}

	var updateTime int64
	if global.UpdateTime != nil {
		updateTime = global.UpdateTime.UnixNano()
	}

	var finishTime int64
	if global.FinishTime != nil {
		finishTime = global.FinishTime.UnixNano()
	}

	var rollbackTime int64
	if global.RollbackTime != nil {
		rollbackTime = global.RollbackTime.UnixNano()
	}

	bins := as.BinMap{
		"xid":             txid,
		"gid":             global.Gid,
		"trans_type":      global.TransType,
		"status":          global.Status,
		"query_prepared":  global.QueryPrepared,
		"protocol":        global.Protocol,
		"create_time":     createTime,
		"update_time":     updateTime,
		"finish_time":     finishTime,
		"rollback_time":   rollbackTime,
		"options":         global.Options,
		"custom_data":     global.CustomData,
		"nxt_cron_intrvl": global.NextCronInterval,
		"next_cron_time":  next_cron_time,
		"ext_data":        global.ExtData,
		"owner":           global.Owner,
		"st_nxt_ctime":    cdtStatusNextCronTime,
		"branches":        *branches,
	}

	policy := as.NewWritePolicy(0, 0)
	policy.CommitLevel = as.COMMIT_ALL
	policy.TotalTimeout = 200 * time.Millisecond
	policy.RecordExistsAction = as.UPDATE

	err = client.Put(policy, key, bins)
	if err != nil {
		return err
	}

	logger.Debugf("NewTransGlobal: created gid: %s", global.Gid)
	return nil
}

func CheckTransGlobalTableForGIDExists(gid string) bool {
	client := aerospikeGet()
	defer connectionPools.Put(client)

	policy := &as.BasePolicy{}
	logger.Debugf("CheckTransGlobalTableForGID: gid being retrieved: %s", gid)

	key, err := as.NewKey(SCHEMA, TransactionGlobal, gid)
	dtmimp.E2P(err)

	record, err := client.GetHeader(policy, key)
	if err != nil {
		return false
	}
	if record == nil {
		return false
	}
	logger.Debugf("CheckTransGlobalTableForGID: found record gid: %d", record.Key.Value().String())

	return true
}

func convertAerospikeRecordToTransGlobalRecord(asRecord *as.Record) *storage.TransGlobalStore {

	tranRecord := &storage.TransGlobalStore{
		ModelBase: dtmutil.ModelBase{},
		Gid:       asRecord.Bins["gid"].(string),
	}

	if asRecord.Bins["create_time"] != nil {
		createTime := convertASIntInterfaceToTime(asRecord.Bins["create_time"])
		tranRecord.CreateTime = &createTime
	}

	if asRecord.Bins["update_time"] != nil {
		updateTime := convertASIntInterfaceToTime(asRecord.Bins["update_time"])
		tranRecord.UpdateTime = &updateTime
	}

	if asRecord.Bins["trans_type"] != nil {
		tranRecord.TransType = asRecord.Bins["trans_type"].(string)
	}

	if asRecord.Bins["status"] != nil {
		tranRecord.Status = asRecord.Bins["status"].(string)
	}

	if asRecord.Bins["query_prepared"] != nil {
		tranRecord.QueryPrepared = asRecord.Bins["query_prepared"].(string)
	}

	if asRecord.Bins["protocol"] != nil {
		tranRecord.Protocol = asRecord.Bins["protocol"].(string)
	}

	if asRecord.Bins["finish_time"] != nil {
		finishTime := convertASIntInterfaceToTime(asRecord.Bins["finish_time"])
		tranRecord.FinishTime = &finishTime
	}

	if asRecord.Bins["rollback_time"] != nil {

		rollbackTime := convertASIntInterfaceToTime(asRecord.Bins["rollback_time"])
		tranRecord.RollbackTime = &rollbackTime
	}

	if asRecord.Bins["options"] != nil {
		tranRecord.Options = asRecord.Bins["options"].(string)
	}

	if asRecord.Bins["custom_data"] != nil {
		tranRecord.CustomData = asRecord.Bins["custom_data"].(string)
	}

	if asRecord.Bins["next_cron_intvl"] != nil {
		var int64Value int64
		if _, ok := asRecord.Bins["next_cron_intvl"].(int); ok {
			int64Value = int64(asRecord.Bins["next_cron_intvl"].(int))
		} else {
			int64Value = asRecord.Bins["next_cron_intvl"].(int64)
		}

		tranRecord.NextCronInterval = int64Value
	}

	if asRecord.Bins["next_cron_time"] != nil {
		nextCronTime := convertASIntInterfaceToTime(asRecord.Bins["next_cron_time"])
		tranRecord.NextCronTime = &nextCronTime
	}

	if asRecord.Bins["owner"] != nil {
		tranRecord.Owner = asRecord.Bins["owner"].(string)
	}

	return tranRecord
}

func getTransGlobalStore(gid string) *storage.TransGlobalStore {
	client := aerospikeGet()
	defer connectionPools.Put(client)

	policy := &as.BasePolicy{}
	if gid == "" {
		logger.Infof("GetTransGlobal: gid is empty")
	}
	logger.Debugf("GetTransGlobal: gid being retrieved: %s", gid)

	key, err := as.NewKey(SCHEMA, TransactionGlobal, gid)
	dtmimp.E2P(err)

	bins := getTransGlobalTableBins()
	record, err := client.Get(policy, key, *bins...)
	if err != nil {
		logger.Errorf("GetTransGlobal: %s", err)
		return nil
	}

	logger.Debugf("GetTransGlobal: retrieve record gid: %s", record.Bins["gid"])

	transStore := convertAerospikeRecordToTransGlobalRecord(record)

	return transStore
}

func getTransGlobalStoreWithStatus(gid string, status string) (*as.Record, error) {
	client := aerospikeGet()
	defer connectionPools.Put(client)

	gidExp := as.ExpEq(as.ExpStringBin("gid"), as.ExpStringVal(gid))
	statusExp := as.ExpEq(as.ExpStringBin("status"), as.ExpStringVal(status))
	exp := as.ExpAnd(gidExp, statusExp)
	logger.Debugf("getTransGlobalStoreWithStatus: where gid = (%s) and status = (%s)", gid, status)

	policy := as.NewQueryPolicy()
	policy.FilterExpression = exp

	bins := getTransGlobalTableBins()

	statement := &as.Statement{
		Namespace: SCHEMA,
		SetName:   TransactionGlobal,
		IndexName: "TXM_GID",
		BinNames:  *bins,
		Filter:    nil,
		TaskId:    0,
	}

	rs, err := client.Query(policy, statement)

	if err != nil {
		logger.Errorf("getTransGlobalStoreWithStatus error: %s", err)
		return nil, err
	}
	counter := int64(0)
	var foundRecord *as.Record
	for res := range rs.Results() {
		if res.Err != nil {
			logger.Errorf("getTransGlobalStoreWithStatus: %s", res.Err)
			return nil, res.Err
		}
		bins := res.Record.Bins
		logger.Debugf("getTransGlobalStoreWithStatus: gid (%s) status (%s)", bins["gid"].(string), bins["status"].(string))

		if counter < 1 {
			foundRecord = res.Record
			counter++
		} else {
			counter++
			logger.FatalfIf(counter > 1, "getTransGlobalStoreWithStatus more than 1 record found count is upto (%d)", counter)
			break
		}
	}
	return foundRecord, nil
}

func ChangeGlobalStatus(global *storage.TransGlobalStore, newStatus string, updates []string, finished bool) {
	client := aerospikeGet()
	defer connectionPools.Put(client)

	oldStatus := global.Status
	//policy := &as.BasePolicy{}
	logger.Debugf("ChangeGlobalStatus: gid being retrieved: %s trying to set status(%s) finished(%t)", global.Gid, newStatus, finished)

	//key, err := as.NewKey(SCHEMA, TransactionGlobal, global.Gid)
	//dtmimp.E2P(err)

	//bins := getTransGlobalTableBins()
	//record, err := client.Get(policy, key, *bins...)
	record, err := getTransGlobalStoreWithStatus(global.Gid, oldStatus)
	dtmimp.E2P(err)
	currentStatus := record.Bins["status"]
	if currentStatus == oldStatus {
		logger.Debugf("ChangeGlobalStatus: found gid (%s) with old status(%s) and new status(%s)", global.Gid, global.Status, newStatus)
		UpdateGlobalStatus(record, global, newStatus, updates, finished)
	}
}

func UpdateGlobalStatus(record *as.Record, global *storage.TransGlobalStore, newStatus string, updates []string, finished bool) {
	client := aerospikeGet()
	defer connectionPools.Put(client)

	resultRecordBins := record.Bins
	resultRecordBins["status"] = newStatus

	for _, v := range updates {
		switch v {
		case "update_time":
			resultRecordBins["update_time"] = global.UpdateTime.UnixNano()
			logger.Debugf("UpdateGlobalStatus: for gid: %s updating time to (%v)", record.Bins["gid"].(string), global.UpdateTime)
		case "finish_time":
			resultRecordBins["finish_time"] = global.FinishTime.UnixNano()
			logger.Debugf("UpdateGlobalStatus: for gid: %s finish time to (%v)", record.Bins["gid"].(string), global.FinishTime)
		case "status":
			resultRecordBins["status"] = newStatus
			logger.Debugf("UpdateGlobalStatus: for gid: %s status to (%s)", record.Bins["gid"].(string), newStatus)
		case "rollback_time":
			resultRecordBins["rollback_time"] = global.RollbackTime.UnixNano()
			logger.Debugf("UpdateGlobalStatus: for gid: %s rollback_time to (%v)", record.Bins["gid"].(string), global.RollbackTime)

		default:
			logger.Debugf("UpdateGlobalStatus: unhandled field %s", v)
			dtmimp.E2P(errors.New("UpdateGlobalStatus: unhandled field for update"))
		}
	}

	updatePolicy := &as.WritePolicy{
		BasePolicy: as.BasePolicy{
			TotalTimeout: 200 * time.Millisecond,
		},
		RecordExistsAction: as.UPDATE_ONLY,
		GenerationPolicy:   0,
		CommitLevel:        0,
		Generation:         0,
		Expiration:         0,
		RespondPerEachOp:   false,
		DurableDelete:      true,
	}

	err := client.Put(updatePolicy, record.Key, resultRecordBins)
	dtmimp.E2P(err)
}

func BuildTransGlobalScanList() (*as.Record, error) {
	client := aerospikeGet()
	defer connectionPools.Put(client)

	var record *as.Record

	policy := as.NewScanPolicy()
	policy.MaxConcurrentNodes = 0
	policy.IncludeBinData = true

	listTimestamp := time.Now().UnixNano()
	listPolicy := as.NewListPolicy(as.ListOrderOrdered, as.ListWriteFlagsAddUnique)

	listKey, err := as.NewKey(SCHEMA, "listpaganation", listTimestamp)

	if err != nil {
		return nil, err
	}

	ok, err := client.Delete(nil, listKey)
	if err != nil {
		return nil, err
	}

	if ok == true {
		logger.Debugf("BuildTransGlobalScanList: delete okay (%t)", ok)
	}

	// Build new list
	recs, err := client.ScanAll(policy, SCHEMA, TransactionGlobal, "xid", "gid")
	if err != nil {
		logger.Errorf("BuildTransGlobalScanList: %s", err)
		return nil, err
	}

	for rec := range recs.Results() {
		bins := rec.Record.Bins
		txid := bins["xid"]
		gid := bins["gid"]
		var itemList []interface{}
		itemList = append(itemList, txid, gid)
		record, err = client.Operate(nil, listKey, as.ListAppendWithPolicyOp(listPolicy, "xid_list", itemList))
		if err != nil {
			dtmimp.E2P(err)
		}
	}
	return record, nil
}

// Todo clean this code up the pagination is working but it is miserable bad code.
// ScanTransGlobalTable
func ScanTransGlobalTable(position *string, limit int64) (*[]storage.TransGlobalStore, *string) {
	client := aerospikeGet()
	defer connectionPools.Put(client)

	record, err := BuildTransGlobalScanList()
	dtmimp.E2P(err)

	listRecord, err := client.Get(nil, record.Key, "xid_list")
	//listRecord, err := client.Operate(nil, record.Key, as.ListGetByIndexOp("xid_list", 0, as.ListReturnTypeRank, as.CtxListIndex(0)))
	bins := listRecord.Bins
	list := bins["xid_list"].([]interface{})

	var resultList []storage.TransGlobalStore
	var pos string

	if *position == "" {
		index := int64(0)
		for index < limit && index < int64(len(list)) {
			itemList := list[index].([]interface{})
			txid := itemList[0]
			gid := itemList[1]
			logger.Debugf("ScanTransGlobalTable: xid(%v) and gid(%s)", txid, gid)
			tran := getTransGlobalStore(gid.(string))
			resultList = append(resultList, *tran)
			index++
		}
		if index < int64(len(list)) {
			itemList := list[index].([]interface{})
			gid := itemList[1]
			pos = gid.(string)
		}
		logger.Debugf("ScanTransGlobalTable: exited for loop index(%d) and limit(%d) next position(%s)", index, limit, pos)
	} else {
		// Need to go find it in the list
		for i, item := range list {
			itemList := item.([]interface{})
			gid := itemList[1]
			if gid == *position {
				logger.Debugf("ScanTransGlobalTable: found requested position(%s) at index(%d)", *position, i)
				index := int64(i)
				if index+1 < int64(len(list)) {
					index++
				} else {
					logger.Debugf("ScanTransGlobalTable: breaking out cause we index out of range(%d) array length(%d)", index, int64(len(list)))
					break
				}
				counter := int64(0)
				for counter < limit && index < int64(len(list)) {
					itemList := list[index].([]interface{})
					txid := itemList[0]
					gid := itemList[1]
					logger.Debugf("ScanTransGlobalTable: xid(%v) and gid(%s)", txid, gid)
					tran := getTransGlobalStore(gid.(string))
					resultList = append(resultList, *tran)
					counter++
					if counter < limit {
						index++
					}
				}
				length := (int64(len(list)))
				logger.Debugf("ScanTransGlobalTable: found requested length(%d) at index(%d)", length, index)
				if index < length {
					itemList := list[index].([]interface{})
					gid := itemList[1]
					pos = gid.(string)
					logger.Debugf("ScanTransGlobalTable: reached limit(%d) setting pos (%s)", limit, pos)
				} else {
					pos = ""
				}
			}
		}
	}

	return &resultList, &pos
}

func LockOneGlobalTransTrans(expireIn time.Duration) *storage.TransGlobalStore {

	client := aerospikeGet()
	defer connectionPools.Put(client)

	expiredTimeTime := time.Now().Add(expireIn)
	expiredTime := expiredTimeTime.UnixNano()
	logger.Debugf("LockOneGlobalTrans: where expired less then: %d, realtime (%s)", expiredTime, expiredTimeTime.String())
	owner := shortuuid.New()

	policy := as.NewQueryPolicy()
	bins := getTransGlobalTableBins()
	//policy.MaxRecords = int64(1)

	statement := &as.Statement{
		Namespace: SCHEMA,
		SetName:   TransactionGlobal,
		IndexName: "st_nxt_ctime",
		BinNames:  *bins,
		Filter:    nil,
		TaskId:    0,
	}

	rs, err := client.Query(policy, statement)

	if err != nil {
		logger.Errorf("LockOneGlobalTrans error: %s", err)
		return nil
	}

	updatePolicy := &as.WritePolicy{
		BasePolicy: as.BasePolicy{
			TotalTimeout: 200 * time.Millisecond,
		},
		RecordExistsAction: as.UPDATE_ONLY,
		GenerationPolicy:   0,
		CommitLevel:        0,
		Generation:         0,
		Expiration:         0,
		RespondPerEachOp:   false,
		DurableDelete:      true,
	}
	counter := int64(0)
	for res := range rs.Results() {
		if res.Err != nil {
			logger.Errorf("LockOneGlobalTrans: %s", res.Err)
			return nil
		}
		id := res.Record.Key
		bins := res.Record.Bins
		status := bins["status"].(string)
		nextCronTime := convertASIntInterfaceToTime(bins["next_cron_time"])

		//logger.Debugf("LockOneGlobalTrans: gid (%s) expires (%s)", bins["gid"].(string), nextCronTime.String())

		if (nextCronTime.UnixNano() < expiredTime) && (status == "prepared" || status == "aborting" || status == "submitted") {
			if counter < 1 {
				logger.Debugf("LockOneGlobalTrans: record found gid(%s) status (%s)", bins["gid"].(string), bins["status"].(string))
				next := time.Now().Add(time.Duration(conf.RetryInterval) * time.Second).UnixNano()
				bins["update_time"] = time.Now().UnixNano()
				bins["next_cron_time"] = next
				bins["owner"] = owner
				err = client.Put(updatePolicy, id, bins)

				if err != nil {
					logger.Errorf("LockOneGlobalTrans: error updating global transaction id %v, bins:%v", id, bins)
					dtmimp.E2P(err)
				}
				logger.Debugf("LockOneGlobalTrans: locking a trans: %v", bins) // with gid %s and owner %s", bins["gid"].(string), owner")
				// found the first one
				// Now go retrieve it and return using the key

				policy := &as.BasePolicy{}

				bins := getTransGlobalTableBins()
				record, err := client.Get(policy, res.Record.Key, *bins...)
				if err != nil {
					logger.Errorf("LockOneGlobalTrans: retrieved found trans gid(%s)", res.Record.Bins["gid"].(string))
					return nil
				}
				logger.Debugf("LockOneGlobalTrans: record: %v", record) // with gid %s and owner %s", bins["gid"].(string), owner")
				resultTrans := convertAerospikeRecordToTransGlobalRecord(record)
				counter++
				return resultTrans

			} else {
				break
			}
		}
	}
	return nil
}

// Todo review and optimize this code.
// ResetCronTimeGlobalTran
func ResetCronTimeGlobalTran(timeout time.Duration, limit int64) (succeedCount int64, hasRemaining bool, err error) {
	logger.Debugf("ResetCronTimeGlobalTran: timeout %v limit(%d)", timeout, limit)
	client := aerospikeGet()
	defer connectionPools.Put(client)

	policy := as.NewQueryPolicy()

	expiredTime := time.Now().Add(timeout)
	expired := expiredTime.UnixNano()
	logger.Debugf("ResetCronTimeGlobalTran: where expired greater then: %d, realtime (%s)", expired, expiredTime.String())

	whereTime := as.ExpGreater(as.ExpIntBin("next_cron_time"), as.ExpIntVal(expired))
	//contains1 := as.ExpEq(as.ExpStringBin("status"), as.ExpStringVal("prepared"))
	//contains2 := as.ExpEq(as.ExpStringBin("status"), as.ExpStringVal("aborting"))
	//contains3 := as.ExpEq(as.ExpStringBin("status"), as.ExpStringVal("submitted"))
	//
	//statusExp := as.ExpIntOr(contains1, contains2, contains3)
	//
	//expression := as.ExpAnd(whereTime, statusExp)

	policy.FilterExpression = whereTime
	var bins = []string{"gid", "status", "next_cron_time"}
	statement := &as.Statement{
		Namespace: SCHEMA,
		SetName:   TransactionGlobal,
		IndexName: "st_nxt_ctime",
		BinNames:  bins,
		Filter:    nil,
		TaskId:    0,
	}

	// Todo complain about MaxRecords being an approximation

	rs, err := client.Query(policy, statement)
	if err != nil {
		logger.Errorf("ResetCronTimeGlobalTran: query error: %s", err)
		return 0, false, err
	}

	updatePolicy := &as.WritePolicy{
		BasePolicy: as.BasePolicy{
			TotalTimeout: 200 * time.Millisecond,
		},
		RecordExistsAction: as.UPDATE,
		GenerationPolicy:   0,
		CommitLevel:        0,
		Generation:         0,
		Expiration:         0,
		RespondPerEachOp:   false,
		DurableDelete:      true,
	}

	succeedCount = 0
	resultCounter := int64(0)
	for res := range rs.Results() {
		if res.Err != nil {
			logger.Errorf("ResetCronTimeGlobalTran: results error: %s", err)
			dtmimp.E2P(res.Err)
		}
		if resultCounter < limit {
			id := res.Record.Key
			bins := res.Record.Bins
			status := bins["status"].(string)
			expireValue := convertASIntInterfaceToTime(bins["next_cron_time"])
			logger.Debugf("ResetCronTimeGlobalTran: gid (%s) expires (%s)", bins["gid"].(string), expireValue.String())
			if status == "prepared" || status == "aborting" || status == "submitted" {
				bins["next_cron_time"] = dtmutil.GetNextTime(0).UnixNano()
				err = client.Put(updatePolicy, id, bins)
				if err != nil {
					logger.Errorf("error updating global transaction id %v, bins:%v", id, bins)
					dtmimp.E2P(err)
				}
				succeedCount++
			}
		}
		resultCounter++
	}

	logger.Debugf("ResetCronTimeGlobalTran: succeedCount (%d) resultsCounter(%d)", succeedCount, resultCounter)
	if succeedCount == 0 {
		hasRemaining = false
		return
	}

	if succeedCount <= limit && succeedCount != 0 {
		logger.Debugf("ResetCronTimeGlobalTran: succeedCount(%d) == limit(%d)", succeedCount, limit)
		//policy.MaxRecords = int64(1)
		rs2, err := client.Query(policy, statement)
		if err != nil {
			logger.Errorf("ResetCronTimeGlobalTran: query error: %s", err)
			return 0, false, err
		}

		counter := 0
		for res := range rs2.Results() {

			if res.Err != nil {
				logger.Errorf("ResetCronTimeGlobalTran: results error: %s", err)
				dtmimp.E2P(res.Err)
			}
			bins := res.Record.Bins
			status := bins["status"].(string)

			if status == "prepared" || status == "aborting" || status == "submitted" {
				counter++
			}

		}

		if succeedCount < limit {
			hasRemaining = true
		}

		if counter > 0 {
			logger.Debugf("ResetCronTimeGlobalTran: checking for remaining counter = %d remaining %t", counter, hasRemaining)
			hasRemaining = true
		} else {
			hasRemaining = false
			logger.Debugf("ResetCronTimeGlobalTran: checking for remaining counter = %d remaining %t", counter, hasRemaining)
		}
	}

	return
}

func TouchCronTimeGlobalTran(global *storage.TransGlobalStore, nextCronInterval int64, nextCronTime *time.Time) {
	client := aerospikeGet()
	defer connectionPools.Put(client)

	policy := as.NewQueryPolicy()

	filter := as.NewEqualFilter("gid", global.Gid)

	var bins = []string{"gid", "status", "update_time", "next_cron_time", "nxt_cron_intrvl"}
	statement := &as.Statement{
		Namespace: SCHEMA,
		SetName:   TransactionGlobal,
		IndexName: "TXM_GID",
		BinNames:  bins,
		Filter:    filter,
		TaskId:    0,
	}

	rs, err := client.Query(policy, statement)
	dtmimp.E2P(err)

	updatePolicy := &as.WritePolicy{
		BasePolicy: as.BasePolicy{
			TotalTimeout: 200 * time.Millisecond,
		},
		RecordExistsAction: as.UPDATE_ONLY,
		GenerationPolicy:   0,
		CommitLevel:        0,
		Generation:         0,
		Expiration:         0,
		RespondPerEachOp:   false,
		DurableDelete:      true,
	}

	for res := range rs.Results() {
		id := res.Record.Key
		resBin := res.Record.Bins
		resBin["status"] = global.Status
		resBin["update_time"] = dtmutil.GetNextTime(0).UnixNano()
		resBin["next_cron_time"] = nextCronTime.UnixNano()
		resBin["nxt_cron_intrvl"] = nextCronInterval

		err = client.Put(updatePolicy, id, resBin)
		if err != nil {
			logger.Errorf("error updating global transaction id %v, bins:%v", id, resBin)
			dtmimp.E2P(err)
		}
	}
}

func DropTableTransBranchOp() {
	//drop table IF EXISTS dtm.trans_global;
	client := aerospikeGet()
	defer connectionPools.Put(client)

	writePolicy := &as.WritePolicy{
		RecordExistsAction: as.REPLACE,
		DurableDelete:      true,
		CommitLevel:        as.COMMIT_MASTER,
	}
	now := time.Now()
	err := client.Truncate(writePolicy, SCHEMA, TransactionBranchOp, &now)
	dtmimp.E2P(err)

	err = client.DropIndex(writePolicy, SCHEMA, TransactionBranchOp, "TXMBRANCH_XID")
	dtmimp.E2P(err)

	err = client.DropIndex(writePolicy, SCHEMA, TransactionBranchOp, "TXMBRANCH_GID")
	dtmimp.E2P(err)

	err = client.DropIndex(writePolicy, SCHEMA, TransactionBranchOp, "GID_BRANCH_UNIQ")
	dtmimp.E2P(err)

}

func CreateTransBranchOpSet() {
	//CREATE SEQUENCE if not EXISTS dtm.trans_branch_op_seq;
	//CREATE TABLE IF NOT EXISTS dtm.trans_branch_op (
	//id bigint NOT NULL DEFAULT NEXTVAL ('dtm.trans_branch_op_seq'),
	//gid varchar(128) NOT NULL,
	//url varchar(1024) NOT NULL,
	//data TEXT,
	//bin_data bytea,
	//branch_id VARCHAR(128) NOT NULL,
	//op varchar(45) NOT NULL,
	//status varchar(45) NOT NULL,
	//finish_time timestamp(0) with time zone DEFAULT NULL,
	//rollback_time timestamp(0) with time zone DEFAULT NULL,
	//create_time timestamp(0) with time zone DEFAULT NULL,
	//update_time timestamp(0) with time zone DEFAULT NULL,
	//PRIMARY KEY (id),
	//CONSTRAINT gid_branch_uniq UNIQUE (gid, branch_id, op)
	//);

	client := aerospikeGet()
	defer connectionPools.Put(client)

	var transBranch storage.TransBranchStore

	txid := xid.New()
	key, err := as.NewKey(SCHEMA, TransactionBranchOp, txid.Bytes())
	dtmimp.E2P(err)

	gid_branch_uniq := map[string]interface{}{
		"gid":       transBranch.Gid,
		"branch_id": transBranch.BranchID,
		"op":        transBranch.Op,
	}

	bins := as.BinMap{}
	bins["xid"] = txid
	bins["gid"] = transBranch.Gid
	bins["url"] = transBranch.URL
	//bins["data"] = transBranch.
	bins["bin_data"] = transBranch.BinData
	bins["branch_id"] = transBranch.BranchID
	bins["op"] = transBranch.Op
	bins["status"] = transBranch.Status
	bins["finish_time"] = 0
	bins["create_time"] = 0
	bins["update_time"] = 0
	bins["gid_branch_uniq"] = gid_branch_uniq

	policy := as.NewWritePolicy(0, 0)
	policy.CommitLevel = as.COMMIT_ALL
	policy.TotalTimeout = 200 * time.Millisecond

	err = client.Put(policy, key, bins)
	dtmimp.E2P(err)

	//create index if not EXISTS gid on dtm.trans_global(gid);
	indexTask, err := client.CreateIndex(policy, SCHEMA, TransactionBranchOp, "TXMBRANCH_GID", "gid", as.STRING)
	dtmimp.E2P(err)
	Idxerr := <-indexTask.OnComplete()
	dtmimp.E2P(Idxerr)

	//create index on XID;
	indexTaskXid, err := client.CreateIndex(policy, SCHEMA, TransactionBranchOp, "TXMBRANCH_XID", "xid", as.STRING)
	dtmimp.E2P(err)
	IdxerrXid := <-indexTaskXid.OnComplete()
	dtmimp.E2P(IdxerrXid)

	//CONSTRAINT gid_branch_uniq UNIQUE (gid, branch_id, op)
	indexTask1, err := client.CreateComplexIndex(policy, SCHEMA, TransactionBranchOp, "GID_BRANCH_UNIQ", "gid_branch_uniq", as.STRING, as.ICT_MAPVALUES)
	Idxerr1 := <-indexTask1.OnComplete()
	dtmimp.E2P(Idxerr1)

	now := time.Now()
	errTruncate := client.Truncate(policy, SCHEMA, TransactionBranchOp, &now)
	dtmimp.E2P(errTruncate)
}

// getTransGlobalBranches
// @Description: get the branchs bin for a Global Transaction and returns a list of xids for branches
func getTransGlobalBranches(gid string) *[]xid.ID {
	client := aerospikeGet()
	defer connectionPools.Put(client)

	policy := &as.BasePolicy{}
	if gid == "" {
		logger.Infof("GetTransGlobal: gid is empty")
	}
	logger.Debugf("GetTransGlobal: gid being retrieved: %s", gid)

	key, err := as.NewKey(SCHEMA, TransactionGlobal, gid)
	dtmimp.E2P(err)

	bins := getTransGlobalTableBins()
	record, err := client.Get(policy, key, *bins...)
	if err != nil {
		logger.Errorf("GetTransGlobal: %s", err)
		return nil
	}

	var branchList []xid.ID
	if record.Bins["branches"] != nil {
		bl := record.Bins["branches"].([]interface{})

		for _, b := range bl {
			xidBytes := b.([]byte)
			txid, err := xid.FromBytes(xidBytes)
			if err != nil {
				logger.Errorf("GetTransGlobal: %s", err)
				return nil
			}
			branchList = append(branchList, txid)
		}

	}

	return &branchList
}

func getBranchOpSetBins() *[]string {
	bins := []string{
		"xid",
		"gid",
		"url",
		"bin_data",
		"branch_id",
		"op",
		"status",
		"finish_time",
		"rollback_time",
		"create_time",
		"update_time",
		"gid_branch_uniq",
	}
	return &bins
}

func newTransBranchOpSet(branches []storage.TransBranchStore) (*[]xid.ID, error) {
	client := aerospikeGet()
	defer connectionPools.Put(client)

	policy := as.NewWritePolicy(0, 0)
	policy.CommitLevel = as.COMMIT_ALL
	policy.TotalTimeout = 200 * time.Millisecond
	policy.RecordExistsAction = as.UPDATE

	var branchXIDList []xid.ID

	for _, branch := range branches {

		txid := xid.New()
		key, err := as.NewKey(SCHEMA, TransactionBranchOp, txid.Bytes())
		//key, err := as.NewKey(SCHEMA, TransactionBranchOp, branch.BranchID)
		dtmimp.E2P(err)

		gid_branch_uniq := map[string]interface{}{
			"gid":       branch.Gid,
			"branch_id": branch.BranchID,
			"op":        branch.Op,
		}

		var finishTime int64
		if branch.FinishTime != nil {
			finishTime = branch.FinishTime.UnixNano()
		}

		var rollbackTime int64
		if branch.RollbackTime != nil {
			rollbackTime = branch.RollbackTime.UnixNano()
		}

		var createTime int64
		if branch.CreateTime != nil {
			createTime = branch.CreateTime.UnixNano()
		}

		var updateTime int64
		if branch.UpdateTime != nil {
			updateTime = branch.UpdateTime.UnixNano()
		}

		bins := as.BinMap{
			"xid":             txid,
			"gid":             branch.Gid,
			"url":             branch.URL,
			"bin_data":        branch.BinData,
			"branch_id":       branch.BranchID,
			"op":              branch.Op,
			"status":          branch.Status,
			"finish_time":     finishTime,
			"rollback_time":   rollbackTime,
			"create_time":     createTime,
			"update_time":     updateTime,
			"gid_branch_uniq": gid_branch_uniq,
		}

		err = client.Put(policy, key, bins)
		if err != nil {
			return nil, err
		}

		branchXIDList = append(branchXIDList, txid)
	}
	return &branchXIDList, nil
}

// updateBranch
func updateBranch(gid string, branch storage.TransBranchStore) error {
	logger.Debugf("updateBranch: update branch gid(%s) branch_id (%s) op (%s) status(%s)", branch.Gid, branch.BranchID, branch.Op, branch.Status)
	client := aerospikeGet()
	defer connectionPools.Put(client)

	policy := as.NewQueryPolicy()
	gidExp := as.ExpEq(as.ExpStringBin("gid"), as.ExpStringVal(gid))
	branchExp := as.ExpEq(as.ExpStringBin("branch_id"), as.ExpStringVal(branch.BranchID))
	opExp := as.ExpEq(as.ExpStringBin("op"), as.ExpStringVal(branch.Op))
	filterExp := as.ExpAnd(gidExp, branchExp, opExp)

	policy.FilterExpression = filterExp

	var bins = getBranchOpSetBins()
	statement := &as.Statement{
		Namespace: SCHEMA,
		SetName:   TransactionBranchOp,
		IndexName: "GID_BRANCH_UNIQ",
		BinNames:  *bins,
		Filter:    nil,
		TaskId:    0,
	}

	rs, err := client.Query(policy, statement)
	dtmimp.E2P(err)
	counter := int(0)
	for res := range rs.Results() {

		if res.Err != nil {
			// handle error here
			logger.Errorf("unable to read record, %s", res.Err)
			// if you want to exit, cancel the recordset to release the resources
			err := rs.Close()
			if err != nil {
				continue
			}
		} else {
			id := res.Record.Key
			logger.Infof("retrieved key: %s", id)

			bins := res.Record.Bins

			gid_branch_uniq := map[string]interface{}{
				"gid":       branch.Gid,
				"branch_id": branch.BranchID,
				"op":        branch.Op,
			}
			// Don't override the transaction ID
			bins["url"] = branch.URL
			bins["bin_data"] = branch.BinData
			bins["op"] = branch.Op
			bins["status"] = branch.Status
			bins["finish_time"] = branch.FinishTime.UnixNano()
			bins["create_time"] = branch.CreateTime.UnixNano()
			bins["update_time"] = branch.UpdateTime.UnixNano()
			bins["gid_branch_uniq"] = gid_branch_uniq
			logger.Debugf("updateBranch: update branch gid(%s) branch_id (%s) status(%s)", branch.Gid, branch.BranchID, branch.Status)
			updatePolicy := &as.WritePolicy{
				BasePolicy: as.BasePolicy{
					TotalTimeout: 200 * time.Millisecond,
				},
				RecordExistsAction: as.UPDATE_ONLY,
				GenerationPolicy:   0,
				CommitLevel:        0,
				Generation:         0,
				Expiration:         0,
				RespondPerEachOp:   false,
				DurableDelete:      true,
			}
			err = client.Put(updatePolicy, id, bins)
			if err != nil {
				logger.Errorf("updateBranch: %s", err)
				return err
			}
			counter++
		}
	}
	logger.Debugf("updateBranch: updated %d records", counter)

	return nil
}

func GetBranchs(gid string) *[]storage.TransBranchStore {
	client := aerospikeGet()
	defer connectionPools.Put(client)

	policy := &as.BasePolicy{}

	branchList := getTransGlobalBranches(gid)
	if branchList == nil {
		return nil
	}

	var results []storage.TransBranchStore
	var bins = getBranchOpSetBins()
	for _, branch := range *branchList {
		txid := branch
		key, err := as.NewKey(SCHEMA, TransactionBranchOp, txid.Bytes())
		if err != nil {
			logger.Errorf("GetBranches: %s", err)
			return nil
		}
		record, err := client.Get(policy, key, *bins...)
		if err != nil {
			logger.Errorf("GetBranches: %s", err)
			return nil
		}
		logger.Debugf("retrieved record: txid(%v)", record.Key.String())
		result := convertASRecordToTransBranchStore(record)
		results = append(results, *result)
	}

	return &results
}

func convertASRecordToTransBranchStore(asRecord *as.Record) *storage.TransBranchStore {

	tranBranch := storage.TransBranchStore{
		ModelBase: dtmutil.ModelBase{},
		Gid:       asRecord.Bins["gid"].(string),
		URL:       asRecord.Bins["url"].(string),
		BinData:   asRecord.Bins["bin_data"].([]byte),
		BranchID:  asRecord.Bins["branch_id"].(string),
		Op:        asRecord.Bins["op"].(string),
		Status:    asRecord.Bins["status"].(string),
		//FinishTime:   &finishTime,
		//RollbackTime: &rollbackTime,
	}

	if asRecord.Bins["create_time"] != nil {
		createTime := convertASIntInterfaceToTime(asRecord.Bins["create_time"])
		tranBranch.CreateTime = &createTime
	}

	if asRecord.Bins["update_time"] != nil {
		updateTime := convertASIntInterfaceToTime(asRecord.Bins["update_time"])
		tranBranch.CreateTime = &updateTime
	}

	if asRecord.Bins["finish_time"] != nil {
		finishTime := convertASIntInterfaceToTime(asRecord.Bins["finish_time"])
		tranBranch.FinishTime = &finishTime
	}

	if asRecord.Bins["rollback_time"] != nil {
		rollbackTime := convertASIntInterfaceToTime(asRecord.Bins["rollback_time"])
		tranBranch.FinishTime = &rollbackTime
	}

	return &tranBranch
}

func UpdateBranchsWithGIDStatus(gid string, status string, branches []storage.TransBranchStore) error {
	logger.Debugf("UpdateBranchsWithGIDStatus: gid(%s) status (%s)", gid, status)
	client := aerospikeGet()
	defer connectionPools.Put(client)

	policy := as.NewQueryPolicy()
	gidExp := as.ExpEq(as.ExpStringBin("gid"), as.ExpStringVal(gid))
	statusExp := as.ExpEq(as.ExpStringBin("status"), as.ExpStringVal(status))
	filterExp := as.ExpAnd(gidExp, statusExp)

	policy.FilterExpression = filterExp

	var bins = []string{"xid", "gid"}
	statement := &as.Statement{
		Namespace: SCHEMA,
		SetName:   TransactionGlobal,
		IndexName: "TXM_GID",
		BinNames:  bins,
		Filter:    nil,
		TaskId:    0,
	}

	rs, err := client.Query(policy, statement)
	dtmimp.E2P(err)

	var globalGid string
	counter := int64(0)
	for rec := range rs.Results() {
		if rec.Err != nil {
			logger.Errorf("UpdateBranchsWithGIDStatus: %s", err)
			return err
		}
		bins := rec.Record.Bins
		globalGid = bins["gid"].(string)
		txid := bins["xid"]
		logger.Debugf("UpdateBranchsWithGIDStatus: retrieved gid(%s) xid(%v)", globalGid, txid)
		counter++
	}

	if counter == 0 {
		return errors.New(fmt.Sprint("RECORD_NOT_FOUND gid: %s and status %s", gid, status))
	}

	if counter != 1 {
		dtmimp.E2P(errors.New("more than 1 gid returned"))
	}

	for _, branch := range branches {
		err := updateBranch(gid, branch)
		if err != nil {
			return err
		}
	}

	return nil
}
