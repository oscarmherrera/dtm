package aerospikedb

import (
	as "github.com/aerospike/aerospike-client-go/v5"
	"github.com/dtm-labs/dtm/dtmcli/dtmimp"
	"github.com/rs/xid"
)

//CREATE SCHEMA if not EXISTS dtm
///* SQLINES DEMO *** RACTER SET utf8mb4 */
//;

const SCHEMA = "test"
const  TRANS_GLOBAL = "trans_global"
const drop_table = "delete from " + SCHEMA + "." + TRANS_GLOBAL
const TRANS_GLOBAL_SEQ = "trans_global_seq"
const GLOBAL_SEQ = "GLOBAL_SEQ"

func DropTableTransGlobal() {
	//drop table IF EXISTS dtm.trans_global;
	client := aerospikeGet()
	queryPolicy := client.DefaultQueryPolicy
	writePolicy := client.DefaultWritePolicy

	statement := &as.Statement{
		Namespace: SCHEMA,
		SetName:   TRANS_GLOBAL,
		IndexName: "",
		BinNames:  nil,
		Filter:    nil,
		TaskId:    0,
	}
	execTask, err := aerospikeGet().QueryExecute(queryPolicy,writePolicy, statement, as.DeleteOp())
	dtmimp.E2P(err)

	err <- execTask.OnComplete()
	dtmimp.E2P(err)
}

func GetSequenceTransGlobal() int64 {
	//CREATE SEQUENCE if not EXISTS dtm.trans_global_seq;
	// Create Key
	var seq int64
	sequenceKey, err := as.NewKey(SCHEMA,TRANS_GLOBAL,TRANS_GLOBAL_SEQ)
	dtmimp.E2P(err)

	seqBin := as.NewBin(GLOBAL_SEQ, &seq)

	add := as.AddOp(seqBin)
	get := as.GetBinOp(GLOBAL_SEQ)


	writePolicy := aerospikeGet().DefaultWritePolicy
	record, err := aerospikeGet().Operate(writePolicy,sequenceKey,add,get)
	dtmimp.E2P(err)
	newSeq, ok := record.Bins[GLOBAL_SEQ].(int64)
	if ok == true {
		return newSeq
	}
	return -1

}

func CreateTransGlobalTable () {
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
	//	next_cron_interval int default null,
	//next_cron_time timestamp(0) with time zone default null,
	//owner varchar(128) not null default '',
	//ext_data text,
	//PRIMARY KEY (id),
	//CONSTRAINT gid UNIQUE (gid)
	//);
	id := xid.New()
	key, err := as.NewKey(SCHEMA,TRANS_GLOBAL,id.String())
	dtmimp.E2P(err)

	var bins  as.BinMap
	bins["gid"] =
}

create index if not EXISTS owner on dtm.trans_global(owner);
create index if not EXISTS status_next_cron_time on dtm.trans_global (status, next_cron_time);
drop table IF EXISTS dtm.trans_branch_op;
-- SQLINES LICENSE FOR EVALUATION USE ONLY
CREATE SEQUENCE if not EXISTS dtm.trans_branch_op_seq;
CREATE TABLE IF NOT EXISTS dtm.trans_branch_op (
id bigint NOT NULL DEFAULT NEXTVAL ('dtm.trans_branch_op_seq'),
gid varchar(128) NOT NULL,
url varchar(1024) NOT NULL,
data TEXT,
bin_data bytea,
branch_id VARCHAR(128) NOT NULL,
op varchar(45) NOT NULL,
status varchar(45) NOT NULL,
finish_time timestamp(0) with time zone DEFAULT NULL,
rollback_time timestamp(0) with time zone DEFAULT NULL,
create_time timestamp(0) with time zone DEFAULT NULL,
update_time timestamp(0) with time zone DEFAULT NULL,
PRIMARY KEY (id),
CONSTRAINT gid_branch_uniq UNIQUE (gid, branch_id, op)
);
