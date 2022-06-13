package aerospikedb

import (
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

type TransGlobal struct {
	id              xid.ID
	gid             string //[128]byte
	trans_type      string //[45]byte
	status          string //[45]byte
	query_prepared  string //[128]byte
	protocol        string //[45]byte
	create_time     int64
	update_time     int64
	finish_time     int64
	rollback_time   int64
	options         string //[1024]byte
	custom_data     string //[256]byte
	nxt_cron_intrvl int
	next_cron_time  int64
	ext_data        TEXT
	owner           string //[256]byte
}

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

	var trans TransGlobal

	trans.id = xid.New()
	key, err := as.NewKey(SCHEMA, TransactionGlobal, trans.id.String())
	dtmimp.E2P(err)

	cdtStatusNextCronTime := map[string]interface{}{
		"status":         trans.status,
		"next_cron_time": trans.next_cron_time,
	}

	branches := []string{}

	bins := as.BinMap{
		"xid":             trans.id.Bytes(),
		"gid":             trans.gid,
		"trans_type":      trans.trans_type,
		"status":          trans.status,
		"query_prepared":  trans.query_prepared,
		"protocol":        trans.protocol,
		"create_time":     trans.create_time,
		"update_time":     trans.update_time,
		"finish_time":     trans.finish_time,
		"rollback_time":   trans.rollback_time,
		"options":         trans.options,
		"custom_data":     trans.custom_data,
		"next_cron_intvl": trans.nxt_cron_intrvl,
		"next_cron_time":  trans.next_cron_time,
		"owner":           trans.owner,
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

func NewTransGlobal(global *storage.TransGlobalStore, branches *[]string) error {
	client := aerospikeGet()
	defer connectionPools.Put(client)

	var trans TransGlobal
	logger.Debugf("NewTransGlobal: with gid %s", global.Gid)
	trans.id = xid.New()
	key, err := as.NewKey(SCHEMA, TransactionGlobal, global.Gid)
	dtmimp.E2P(err)

	next_cron_time := global.NextCronTime.UnixNano()
	now := time.Now().UnixNano()

	cdtStatusNextCronTime := map[string]interface{}{
		"status":         global.Status,
		"next_cron_time": next_cron_time,
	}

	bins := as.BinMap{
		"xid":            trans.id.Bytes(),
		"gid":            global.Gid,
		"status":         global.Status,
		"create_time":    now,
		"next_cron_time": next_cron_time,
		"st_nxt_ctime":   cdtStatusNextCronTime,
		"branches":       *branches,
	}

	policy := as.NewWritePolicy(0, 0)
	policy.CommitLevel = as.COMMIT_ALL
	policy.TotalTimeout = 200 * time.Millisecond
	policy.RecordExistsAction = as.REPLACE

	err = client.Put(policy, key, bins)
	if err != nil {
		return err
	}

	logger.Debugf("NewTransGlobal: create gid: %s", global.Gid)
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

// convertASIntInterfaceToTime
// Converts an aerospike integer interface to a go time value
func convertASIntInterfaceToTime(asIntf interface{}) time.Time {
	var timeValue int64

	if _, ok := asIntf.(int); ok {
		timeValue = int64(asIntf.(int))
	} else {
		timeValue = asIntf.(int64)
	}
	value := time.Unix(0, timeValue)
	return value
}

func GetTransGlobal(gid string) *string {
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
	resultString := transStore.String()
	logger.Debugf("GetTransGlobal: raw as record: %s", record.String())
	logger.Debugf("GetTransGlobal: retrieve record result string: %s", resultString)

	return &resultString
}

func UpdateGlobalStatus(global *storage.TransGlobalStore, newStatus string, updates []string, finished bool) {
	client := aerospikeGet()
	defer connectionPools.Put(client)

	policy := &as.BasePolicy{}
	logger.Debugf("UpdateGlobalStatus: gid being retrieved: %s", global.Gid)

	key, err := as.NewKey(SCHEMA, TransactionGlobal, global.Gid)
	dtmimp.E2P(err)

	bins := getTransGlobalTableBins()
	record, err := client.Get(policy, key, *bins...)
	dtmimp.E2P(err)
	logger.Debugf("UpdateGlobalStatus: retrieve record gid: %d", record.Bins["gid"])
	resultRecordBins := record.Bins
	resultRecordBins["status"] = newStatus
	if finished == true {
		resultRecordBins["finish_time"] = time.Now().UnixNano()
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
	err = client.Put(updatePolicy, record.Key, resultRecordBins)
	dtmimp.E2P(err)
}

// Todo Implement as a sorted set
// current I just get the next one in the list which may not be in key order
// Implement pagination as well when a position is passed in that is not empty
func ScanTransGlobalTable(position *string, limit int64) (*[]string, *string) {
	client := aerospikeGet()
	defer connectionPools.Put(client)

	policy := as.NewScanPolicy()
	policy.MaxConcurrentNodes = 0
	policy.IncludeBinData = true
	policy.MaxRecords = limit + 1

	recs, err := client.ScanAll(policy, SCHEMA, TransactionGlobal)

	if err != nil {
		logger.Errorf("ScanTransGlobalTable: %s", err)
		return nil, nil
	}

	var results []string
	var pos string
	var counter = int64(0)
	for res := range recs.Results() {

		if res.Err != nil {
			// handle error here
			logger.Errorf("unable to read record, %s", res.Err)
			// if you want to exit, cancel the recordset to release the resources
			err := recs.Close()
			if err != nil {
				continue
			}
		} else {
			if counter < limit {
				logger.Debugf("ScanTransGlobalTable: record counter %d", counter)
				trans := convertAerospikeRecordToTransGlobalRecord(res.Record)
				//id := res.Record.Key.Value().String()
				logger.Infof("ScanTransGlobalTable: retrieved key: %s with gid", res.Record.Key.String(), res.Record.Bins["gid"])
				logger.Debugf("ScanTransGlobalTable: record retrieved, %v", trans)
				results = append(results, trans.String())
			}
			pos = res.Record.Key.String()
			logger.Debugf("ScanTransGlobalTable: position value, %s", res.Record.Key.String())
		}
		counter++
	}
	if limit > 1 && counter < limit {
		pos = ""
		logger.Debugf("ScanTransGlobalTable: limit is > 1 and counter < %d", limit)
	}
	if limit == 1 && *position != "" {
		pos = ""
		logger.Debugf("ScanTransGlobalTable: limit is 1 and position is not empty")
	}

	return &results, &pos
}

func LockOneGlobalTransString(expireIn time.Duration) *string {
	trans := LockOneGlobalTransTrans(expireIn)
	if trans != nil {
		resultString := trans.String()
		logger.Debugf("LockOneGlobalTrans: locked record %s", resultString)
		return &resultString
	}

	return nil
}

func LockOneGlobalTransTrans(expireIn time.Duration) *storage.TransGlobalStore {

	client := aerospikeGet()
	defer connectionPools.Put(client)

	expiredTime := time.Now().Add(expireIn)
	expired := expiredTime.UnixNano()
	logger.Debugf("LockOneGlobalTrans: where expired less then: %d, realtime (%s)", expired, expiredTime.String())
	owner := shortuuid.New()

	policy := as.NewQueryPolicy()
	//whereTime := as.ExpLess(as.ExpIntBin("next_cron_time"), as.ExpIntVal(expired))
	//contains1 := as.ExpEq(as.ExpStringBin("status"), as.ExpStringVal("prepared"))
	//contains2 := as.ExpEq(as.ExpStringBin("status"), as.ExpStringVal("aborting"))
	//contains3 := as.ExpEq(as.ExpStringBin("status"), as.ExpStringVal("submitted"))

	//statusExp := as.ExpIntOr(contains1, contains2, contains3)

	//expression := as.ExpAnd(whereTime, statusExp)

	//policy.FilterExpression = whereTime

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
		expireValue := convertASIntInterfaceToTime(bins["next_cron_time"])

		logger.Debugf("LockOneGlobalTrans: gid (%s) expires (%s)", bins["gid"].(string), expireValue.String())

		if (status == "prepared" || status == "aborting" || status == "submitted") && (expireValue.UnixNano() < expired) {
			if counter < 1 {
				next := time.Now().Add(time.Duration(conf.RetryInterval) * time.Second).UnixNano()
				bins["next_cron_time"] = next
				bins["owner"] = owner
				err = client.Put(updatePolicy, id, bins)

				if err != nil {
					logger.Errorf("LockOneGlobalTrans: error updating global transaction id %v, bins:%v", id, bins)
					dtmimp.E2P(err)
				}
				logger.Debugf("LockOneGlobalTrans: locking a trans with gid %s and owner %s", bins["gid"].(string), owner)
				// found the first one
				counter++
			} else {
				break
			}
		}
	}
	if counter == 0 {
		// Didn't find anything so return nil
		return nil
	}

	queryPolicy := as.NewQueryPolicy()
	equalOwner := as.ExpEq(as.ExpStringBin("owner"), as.ExpStringVal(owner))
	//filter := as.NewEqualFilter("owner", owner)
	queryPolicy.FilterExpression = equalOwner
	queryPolicy.MaxRecords = 1

	statement = &as.Statement{
		Namespace: SCHEMA,
		SetName:   TransactionGlobal,
		IndexName: "TXM_GID",
		BinNames:  *bins,
		Filter:    nil,
		TaskId:    0,
	}

	rs2, err := client.Query(policy, statement)
	dtmimp.E2P(err)

	var resultTrans *storage.TransGlobalStore
	for res := range rs2.Results() {
		resultTrans = convertAerospikeRecordToTransGlobalRecord(res.Record)
		break
	}

	return resultTrans
}

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
	//if succeedCount < limit && succeedCount != 0 {
	//	hasRemaining = true
	//	return
	//}
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
		RecordExistsAction: as.REPLACE,
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

type TransBranchOp struct {
	id          xid.ID
	gid         string //[128]byte
	url         string //[1024]byte
	data        TEXT
	bin_data    BYTEA
	branch_id   string //[128]byte
	op          string //[45]byte
	status      string //[45]byte
	finish_time int64
	create_time int64
	update_time int64
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

	var transBranch TransBranchOp

	transBranch.id = xid.New()
	key, err := as.NewKey(SCHEMA, TransactionBranchOp, transBranch.id.String())
	dtmimp.E2P(err)

	gid_branch_uniq := map[string]interface{}{
		"gid":       transBranch.gid,
		"branch_id": transBranch.branch_id,
		"op":        transBranch.op,
	}

	bins := as.BinMap{}
	bins["xid"] = transBranch.id
	bins["gid"] = transBranch.gid
	bins["url"] = transBranch.url
	bins["data"] = transBranch.data
	bins["bin_data"] = transBranch.bin_data
	bins["branch_id"] = transBranch.branch_id
	bins["op"] = transBranch.op
	bins["status"] = transBranch.status
	bins["finish_time"] = transBranch.finish_time
	bins["create_time"] = transBranch.create_time
	bins["update_time"] = transBranch.update_time
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
		"create_time",
		"update_time",
		"gid_branch_uniq",
	}
	return &bins
}

func NewTransBranchOpSet(branches []storage.TransBranchStore) error {
	client := aerospikeGet()
	defer connectionPools.Put(client)

	policy := as.NewWritePolicy(0, 0)
	policy.CommitLevel = as.COMMIT_ALL
	policy.TotalTimeout = 200 * time.Millisecond
	policy.RecordExistsAction = as.REPLACE

	for _, branch := range branches {

		xid := xid.New()
		key, err := as.NewKey(SCHEMA, TransactionBranchOp, branch.BranchID)
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

		var createTime int64
		if branch.CreateTime != nil {
			createTime = branch.CreateTime.UnixNano()
		}

		var updateTime int64
		if branch.UpdateTime != nil {
			updateTime = branch.UpdateTime.UnixNano()
		}

		bins := as.BinMap{
			"xid":             xid,
			"gid":             branch.Gid,
			"url":             branch.URL,
			"bin_data":        branch.BinData,
			"branch_id":       branch.BranchID,
			"op":              branch.Op,
			"status":          branch.Status,
			"finish_time":     finishTime,
			"create_time":     createTime,
			"update_time":     updateTime,
			"gid_branch_uniq": gid_branch_uniq,
		}

		err = client.Put(policy, key, bins)
		if err != nil {
			return err
		}
	}
	return nil
}

func GetBranchs(gid string) *[]string {
	client := aerospikeGet()
	defer connectionPools.Put(client)

	policy := as.NewQueryPolicy()

	filter := as.NewEqualFilter("gid", gid)

	bins := getBranchOpSetBins()
	statement := &as.Statement{
		Namespace: SCHEMA,
		SetName:   TransactionBranchOp,
		IndexName: "TXMBRANCH_GID",
		BinNames:  *bins,
		Filter:    filter,
		TaskId:    0,
	}

	rs, err := client.Query(policy, statement)
	dtmimp.E2P(err)

	var results []string

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
			//id := res.Record.Key.Value().String()
			logger.Infof("retrieved record: %v", res.Record)
			result := convertASRecordToTransBranchStoreString(res.Record)
			results = append(results, result)
		}
	}
	return &results
}

func convertASRecordToTransBranchStoreString(asRecord *as.Record) string {

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
	if asRecord.Bins["finish_time"] != nil {
		finishTime := convertASIntInterfaceToTime(asRecord.Bins["finish_time"])
		tranBranch.FinishTime = &finishTime
	}

	if asRecord.Bins["rollback_time"] != nil {
		rollbackTime := convertASIntInterfaceToTime(asRecord.Bins["rollback_time"])
		tranBranch.FinishTime = &rollbackTime
	}

	return tranBranch.String()
}

func UpdateBranchsWithGIDStatus(gid string, status string, branches []storage.TransBranchStore) error {
	client := aerospikeGet()
	defer connectionPools.Put(client)

	policy := as.NewQueryPolicy()
	gidExp := as.ExpEq(as.ExpStringBin("gid"), as.ExpStringVal(gid))
	statusExp := as.ExpEq(as.ExpStringBin("status"), as.ExpStringVal(status))
	filterExp := as.ExpAnd(gidExp, statusExp)

	policy.FilterExpression = filterExp

	bins := getBranchOpSetBins()
	statement := &as.Statement{
		Namespace: SCHEMA,
		SetName:   TransactionBranchOp,
		IndexName: "TXMBRANCH_GID",
		BinNames:  *bins,
		Filter:    nil,
		TaskId:    0,
	}

	branchMap := make(map[string]storage.TransBranchStore, len(branches))
	// Convert BranchList to Map to easily be used later
	for _, branch := range branches {
		branchMap[branch.BranchID] = branch
	}

	rs, err := client.Query(policy, statement)
	dtmimp.E2P(err)

	//var results []string
	//var updateMap map[as.Key]*as.BinMap

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
			b := res.Record.Bins["branch_id"]
			branchId := b.(string)
			branch := branchMap[branchId]

			gid_branch_uniq := map[string]interface{}{
				"gid":       branch.Gid,
				"branch_id": branch.BranchID,
				"op":        branch.Op,
			}

			bins["url"] = branch.URL
			bins["bin_data"] = branch.BinData
			bins["op"] = branch.Op
			bins["status"] = branch.Status
			bins["finish_time"] = branch.FinishTime.UnixNano()
			bins["create_time"] = branch.CreateTime.UnixNano()
			bins["update_time"] = branch.UpdateTime.UnixNano()
			bins["gid_branch_uniq"] = gid_branch_uniq

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
				return err
			}
			//updateMap[*id] = &bins
		}
	}

	return nil
}
