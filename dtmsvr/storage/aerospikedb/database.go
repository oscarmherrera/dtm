package aerospikedb

import (
	"errors"
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

//const TRANS_GLOBAL = "trans_global"
//const TRANS_GLOBAL_SEQ = "trans_global_seq"
//const TRANS_BRANCH_OP = "trans_branch_op"
//const TRANS_BRANCH_OP_SEQ = "trans_branch_op_seq"
//const GLOBAL_SEQ = "GLOBAL_SEQ"

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
	err := client.Truncate(writePolicy, SCHEMA, TransactionManagerSet, &now)
	dtmimp.E2P(err)
	err = client.DropIndex(writePolicy, SCHEMA, TransactionManagerSet, "TXM_GID")
	dtmimp.E2P(err)

	err = client.DropIndex(writePolicy, SCHEMA, TransactionManagerSet, "TXM_OWNER")
	dtmimp.E2P(err)

	err = client.DropIndex(writePolicy, SCHEMA, TransactionManagerSet, "st_nxt_ctime")
	dtmimp.E2P(err)

}

//func GetSequenceTransGlobal() int64 {
//	//CREATE SEQUENCE if not EXISTS dtm.trans_global_seq;
//	// Create Key
//	client := aerospikeGet()
//	defer connectionPools.Put(client)
//
//	var seq int64
//	sequenceKey, err := as.NewKey(SCHEMA, TransactionManagerSet, TRANS_GLOBAL_SEQ)
//	dtmimp.E2P(err)
//
//	seqBin := as.NewBin(GLOBAL_SEQ, &seq)
//
//	add := as.AddOp(seqBin)
//	get := as.GetBinOp(GLOBAL_SEQ)
//
//	writePolicy := client.DefaultWritePolicy
//	record, err := client.Operate(writePolicy, sequenceKey, add, get)
//	dtmimp.E2P(err)
//	newSeq, ok := record.Bins[GLOBAL_SEQ].(int64)
//	if ok == true {
//		return newSeq
//	}
//	return -1
//
//}

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
	key, err := as.NewKey(SCHEMA, TransactionManagerSet, trans.id.String())
	dtmimp.E2P(err)

	cdtStatusNextCronTime := map[string]interface{}{
		"status":         trans.status,
		"next_cron_time": trans.next_cron_time,
	}

	branches := []string{}

	bins := as.BinMap{
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

	//create index if not EXISTS gid on dtm.trans_global(gid);
	indexTask, err := client.CreateIndex(policy, TransactionManagerNamespace, TransactionManagerSet, "TXM_GID", "gid", as.STRING)
	dtmimp.E2P(err)

	Idxerr := <-indexTask.OnComplete()
	dtmimp.E2P(Idxerr)

	//create index if not EXISTS owner on dtm.trans_global(owner);
	indexTask1, err := client.CreateIndex(policy, TransactionManagerNamespace, TransactionManagerSet, "TXM_OWNER", "owner", as.STRING)
	dtmimp.E2P(err)

	Idxerr1 := <-indexTask1.OnComplete()
	dtmimp.E2P(Idxerr1)

	//create index if not EXISTS status_next_cron_time on dtm.trans_global (status, next_cron_time);
	indexTask2, err := client.CreateComplexIndex(policy, TransactionManagerNamespace, TransactionManagerSet, "st_nxt_ctime", "st_nxt_ctime", as.STRING, as.ICT_MAPVALUES)
	Idxerr2 := <-indexTask2.OnComplete()
	dtmimp.E2P(Idxerr2)
}

func NewTransGlobal(global *storage.TransGlobalStore, branches *[]string) error {
	client := aerospikeGet()
	defer connectionPools.Put(client)

	var trans TransGlobal

	trans.id = xid.New()
	key, err := as.NewKey(SCHEMA, TransactionManagerSet, trans.id.String())
	dtmimp.E2P(err)

	next_cron_time := global.NextCronTime.UnixNano()
	now := time.Now().UnixNano()

	cdtStatusNextCronTime := map[string]interface{}{
		"status":         &global.Status,
		"next_cron_time": &next_cron_time,
	}

	var bins as.BinMap
	bins["gid"] = &global.Gid
	bins["status"] = &global.Status
	bins["create_time"] = &now
	bins["next_cron_time"] = &next_cron_time
	bins["st_nxt_ctime"] = &cdtStatusNextCronTime
	bins["branches"] = branches

	policy := as.NewWritePolicy(0, 0)
	policy.CommitLevel = as.COMMIT_ALL
	policy.TotalTimeout = 200 * time.Millisecond

	err = client.Put(policy, key, bins)
	if err != nil {
		return err
	}

	return nil
}

func CheckTransGlobalTableForGID(gid string) bool {
	client := aerospikeGet()
	defer connectionPools.Put(client)

	policy := as.NewQueryPolicy()

	filter := as.NewEqualFilter("gid", gid)

	statement := &as.Statement{
		Namespace: SCHEMA,
		SetName:   TransactionManagerSet,
		IndexName: "TXM_GID",
		BinNames:  nil,
		Filter:    filter,
		TaskId:    0,
	}

	rs, err := client.Query(policy, statement)
	dtmimp.E2P(err)

	if len(rs.Results()) > 0 {
		return true
	}

	return false
}

func GetTransGlobal(gid string) *string {
	client := aerospikeGet()
	defer connectionPools.Put(client)

	policy := as.NewQueryPolicy()

	filter := as.NewEqualFilter("gid", gid)

	bins := getTransGlobalTableBins()
	statement := &as.Statement{
		Namespace: SCHEMA,
		SetName:   TransactionManagerSet,
		IndexName: "TXM_GID",
		BinNames:  *bins,
		Filter:    filter,
		TaskId:    0,
	}

	rs, err := client.Query(policy, statement)
	dtmimp.E2P(err)

	if len(rs.Results()) != 1 {
		dtmimp.E2P(errors.New("multiple records with the same unique gid"))
	}

	var resultString string

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
			id := res.Record.Key.Value().String()
			logger.Infof("retrieved key: %s", id)
			resultString = res.Record.String()
		}
	}
	return &resultString
}

func UpdateGlobalStatus(global *storage.TransGlobalStore, newStatus string, updates []string, finished bool) {
	client := aerospikeGet()
	defer connectionPools.Put(client)

	policy := as.NewQueryPolicy()

	filter := as.NewEqualFilter("gid", global.Gid)

	bins := getTransGlobalTableBins()
	statement := &as.Statement{
		Namespace: SCHEMA,
		SetName:   TransactionManagerSet,
		IndexName: "TXM_GID",
		BinNames:  *bins,
		Filter:    filter,
		TaskId:    0,
	}

	rs, err := client.Query(policy, statement)
	dtmimp.E2P(err)

	if len(rs.Results()) != 1 {
		dtmimp.E2P(errors.New("multiple records with the same unique gid"))
	}

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
			bins["status"] = newStatus
			if finished == true {
				bins["finish_time"] = time.Now().UnixNano()
			}

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
			err = client.Put(updatePolicy, id, bins)
			dtmimp.E2P(err)
		}
	}
}

func ScanTransGlobalTable() *[]string {
	client := aerospikeGet()
	defer connectionPools.Put(client)

	policy := as.NewScanPolicy()
	policy.MaxConcurrentNodes = 0
	policy.IncludeBinData = true

	recs, err := client.ScanAll(policy, SCHEMA, TransactionManagerSet)
	dtmimp.E2P(err)

	if len(recs.Results()) == 0 {
		return nil
	}

	var results []string

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
			id := res.Record.Key.Value().String()
			logger.Infof("retrieved key: %s with gid", id, res.Record.Bins["gid"])
			results = append(results, res.Record.String())
		}
	}
	return &results
}

func LockOneGlobalTrans(expireIn time.Duration) *string {
	client := aerospikeGet()
	defer connectionPools.Put(client)

	expired := time.Now().Add(expireIn).UnixNano()
	owner := shortuuid.New()

	policy := as.NewQueryPolicy()
	whereTime := as.ExpLess(as.ExpIntBin("next_cron_time"), as.ExpIntVal(expired))
	contains1 := as.ExpEq(as.ExpStringBin("status"), as.ExpStringVal("prepared"))
	contains2 := as.ExpEq(as.ExpStringBin("status"), as.ExpStringVal("aborting"))
	contains3 := as.ExpEq(as.ExpStringBin("status"), as.ExpStringVal("submitted"))

	statusExp := as.ExpIntOr(contains1, contains2, contains3)

	expression := as.ExpAnd(whereTime, statusExp)

	policy.FilterExpression = expression

	bins := getTransGlobalTableBins()

	statement := &as.Statement{
		Namespace: SCHEMA,
		SetName:   TransactionManagerSet,
		IndexName: "TXM_GID",
		BinNames:  *bins,
		Filter:    nil,
		TaskId:    0,
	}

	rs, err := client.Query(policy, statement)

	if err != nil {
		return nil
	}

	if len(rs.Results()) == 0 {
		return nil
	}
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
		bins := res.Record.Bins
		next := time.Now().Add(time.Duration(conf.RetryInterval) * time.Second).UnixNano()
		bins["next_cron_time"] = next
		bins["owner"] = owner
		err = aerospikeGet().Put(updatePolicy, id, bins)

		if err != nil {
			logger.Errorf("error updating global transaction id %v, bins:%v", id, bins)
			dtmimp.E2P(err)
		}
	}

	policy = as.NewQueryPolicy()

	filter := as.NewEqualFilter("owner", owner)

	var singleBin = []string{"gid"}
	statement = &as.Statement{
		Namespace: SCHEMA,
		SetName:   TransactionManagerSet,
		IndexName: "TXM_GID",
		BinNames:  singleBin,
		Filter:    filter,
		TaskId:    0,
	}

	rs2, err := client.Query(policy, statement)
	dtmimp.E2P(err)
	var firstGID string
	for res := range rs2.Results() {
		firstGID = res.Record.Bins["gid"].(string)
		break
	}

	resultString := GetTransGlobal(firstGID)

	return resultString
}

func ResetCronTimeGlobalTran(timeout time.Duration, limit int64) (succeedCount int64, hasRemaining bool, err error) {
	client := aerospikeGet()
	defer connectionPools.Put(client)

	timeoutTimestamp := time.Now().Add(timeout).UnixNano()

	policy := as.NewQueryPolicy()
	whereTime := as.ExpGreater(as.ExpIntBin("next_cron_time"), as.ExpIntVal(timeoutTimestamp))
	contains1 := as.ExpEq(as.ExpStringBin("status"), as.ExpStringVal("prepared"))
	contains2 := as.ExpEq(as.ExpStringBin("status"), as.ExpStringVal("aborting"))
	contains3 := as.ExpEq(as.ExpStringBin("status"), as.ExpStringVal("submitted"))

	statusExp := as.ExpIntOr(contains1, contains2, contains3)

	expression := as.ExpAnd(whereTime, statusExp)

	policy.FilterExpression = expression
	var bins = []string{"gid", "NextCronTime"}
	statement := &as.Statement{
		Namespace: SCHEMA,
		SetName:   TransactionManagerSet,
		IndexName: "TXM_GID",
		BinNames:  bins,
		Filter:    nil,
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

	succeedCount = 0

	for res := range rs.Results() {
		id := res.Record.Key
		bins := res.Record.Bins
		bins["next_cron_time"] = dtmutil.GetNextTime(0).UnixNano()
		err = client.Put(updatePolicy, id, bins)
		if err != nil {
			logger.Errorf("error updating global transaction id %v, bins:%v", id, bins)
			dtmimp.E2P(err)
		}
		succeedCount++
	}
	if succeedCount > limit {
		hasRemaining = true
		succeedCount = limit
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
		SetName:   TransactionManagerSet,
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

//func GetSequenceTransBranchOp() int64 {
//	//CREATE SEQUENCE if not EXISTS dtm.trans_global_seq;
//	// Create Key
//	client := aerospikeGet()
//	defer connectionPools.Put(client)
//
//	var seq int64
//	sequenceKey, err := as.NewKey(SCHEMA, TransactionSet, TRANS_BRANCH_OP_SEQ)
//	dtmimp.E2P(err)
//
//	seqBin := as.NewBin(GLOBAL_SEQ, &seq)
//
//	add := as.AddOp(seqBin)
//	get := as.GetBinOp(GLOBAL_SEQ)
//
//	writePolicy := client.DefaultWritePolicy
//	record, err := client.Operate(writePolicy, sequenceKey, add, get)
//	dtmimp.E2P(err)
//	newSeq, ok := record.Bins[GLOBAL_SEQ].(int64)
//	if ok == true {
//		return newSeq
//	}
//	return -1
//
//}

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
	err := client.Truncate(writePolicy, SCHEMA, TransactionSet, &now)
	dtmimp.E2P(err)

	err = client.DropIndex(writePolicy, SCHEMA, TransactionSet, "TXMBRANCH_GID")
	dtmimp.E2P(err)

	err = client.DropIndex(writePolicy, SCHEMA, TransactionSet, "GID_BRANCH_UNIQ")
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
	key, err := as.NewKey(SCHEMA, TransactionManagerSet, transBranch.id.String())
	dtmimp.E2P(err)

	gid_branch_uniq := map[string]interface{}{
		"gid":       transBranch.gid,
		"branch_id": transBranch.branch_id,
		"op":        transBranch.op,
	}

	bins := as.BinMap{}

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
	indexTask, err := client.CreateIndex(policy, SCHEMA, TransactionSet, "TXMBRANCH_GID", "gid", as.STRING)
	dtmimp.E2P(err)
	Idxerr := <-indexTask.OnComplete()
	dtmimp.E2P(Idxerr)

	//CONSTRAINT gid_branch_uniq UNIQUE (gid, branch_id, op)
	indexTask1, err := client.CreateComplexIndex(policy, SCHEMA, TransactionSet, "GID_BRANCH_UNIQ", "gid_branch_uniq", as.STRING, as.ICT_MAPVALUES)
	Idxerr1 := <-indexTask1.OnComplete()
	dtmimp.E2P(Idxerr1)
}

func NewTransBranchOpSet(branches []storage.TransBranchStore) error {
	client := aerospikeGet()
	defer connectionPools.Put(client)

	policy := as.NewWritePolicy(0, 0)
	policy.CommitLevel = as.COMMIT_ALL
	policy.TotalTimeout = 200 * time.Millisecond

	for _, branch := range branches {

		id := xid.New()
		key, err := as.NewKey(SCHEMA, TransactionManagerSet, id.String())
		dtmimp.E2P(err)

		gid_branch_uniq := map[string]interface{}{
			"gid":       &branch.Gid,
			"branch_id": &branch.BranchID,
			"op":        &branch.Op,
		}

		var bins as.BinMap
		bins["gid"] = &branch.Gid
		bins["url"] = &branch.URL
		bins["bin_data"] = &branch.BinData
		bins["branch_id"] = &branch.BranchID
		bins["op"] = &branch.Op
		bins["status"] = &branch.Status
		bins["finish_time"] = &branch.FinishTime
		bins["create_time"] = &branch.CreateTime
		bins["update_time"] = &branch.UpdateTime
		bins["gid_branch_uniq"] = &gid_branch_uniq

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

	bins := getTransGlobalTableBins()
	statement := &as.Statement{
		Namespace: SCHEMA,
		SetName:   TransactionSet,
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
			id := res.Record.Key.Value().String()
			logger.Infof("retrieved key: %s", id)
			results = append(results, res.Record.String())
		}
	}
	return &results
}

func UpdateBranchsWithGIDStatus(gid string, status string, branches []storage.TransBranchStore) error {
	client := aerospikeGet()
	defer connectionPools.Put(client)

	policy := as.NewQueryPolicy()
	gidExp := as.ExpEq(as.ExpStringBin("gid"), as.ExpStringVal(gid))
	statusExp := as.ExpEq(as.ExpStringBin("status"), as.ExpStringVal(status))
	filterExp := as.ExpAnd(gidExp, statusExp)

	policy.FilterExpression = filterExp

	bins := getTransGlobalTableBins()
	statement := &as.Statement{
		Namespace: SCHEMA,
		SetName:   TransactionSet,
		IndexName: "TXMBRANCH_GID",
		BinNames:  *bins,
		Filter:    nil,
		TaskId:    0,
	}

	branchMap := make(map[string]storage.TransBranchStore, len(branches))
	// BranchList to Map
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
				"gid":       &branch.Gid,
				"branch_id": &branch.BranchID,
				"op":        &branch.Op,
			}

			bins["url"] = &branch.URL
			bins["bin_data"] = &branch.BinData
			bins["op"] = &branch.Op
			bins["status"] = &branch.Status
			bins["finish_time"] = &branch.FinishTime
			bins["create_time"] = &branch.CreateTime
			bins["update_time"] = &branch.UpdateTime
			bins["gid_branch_uniq"] = &gid_branch_uniq

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
			err = client.Put(updatePolicy, id, bins)
			if err != nil {
				return err
			}
			//updateMap[*id] = &bins
		}
	}

	return nil
}
