package aerospikedb

import (
	as "github.com/aerospike/aerospike-client-go/v5"
	"github.com/dtm-labs/dtm/dtmcli"
	"github.com/dtm-labs/dtm/dtmcli/dtmimp"
	"github.com/rs/xid"
	"time"
)

func createBarrierSet() {
	//create table if not exists dtm_barrier.barrier(
	//	id bigint NOT NULL DEFAULT NEXTVAL ('dtm_barrier.barrier_seq'),
	//	trans_type varchar(45) default '',
	//gid varchar(128) default '',
	//branch_id varchar(128) default '',
	//op varchar(45) default '',
	//barrier_id varchar(45) default '',
	//reason varchar(45) default '',
	//create_time timestamp(0) with time zone DEFAULT NULL,
	//update_time timestamp(0) with time zone DEFAULT NULL,
	//PRIMARY KEY(id),
	//CONSTRAINT uniq_barrier unique(gid, branch_id, op, barrier_id)
	//);
	client := aerospikeGet()
	defer aerospikePut(client)

	var barrier dtmcli.BranchBarrier

	txid := xid.New()
	key, err := as.NewKey(SCHEMA, BranchBarrierTable, txid.Bytes())
	dtmimp.E2P(err)

	uniq_barrier := map[string]interface{}{
		"gid":        barrier.Gid,
		"branch_id":  barrier.BranchID,
		"op":         barrier.Op,
		"barrier_id": barrier.BarrierID,
	}

	bins := as.BinMap{
		"txid":         txid,
		"trans_type":   barrier.TransType,
		"gid":          barrier.Gid,
		"branch_id":    barrier.BranchID,
		"op":           barrier.Op,
		"barrier_id":   barrier.BarrierID,
		"reason":       "",
		"create_time":  int64(0),
		"update_time":  int64(0),
		"uniq_barrier": uniq_barrier,
	}

	policy := as.NewWritePolicy(0, 0)
	policy.CommitLevel = as.COMMIT_ALL
	policy.TotalTimeout = 200 * time.Millisecond
	policy.RecordExistsAction = as.REPLACE

	err = client.Put(policy, key, bins)
	dtmimp.E2P(err)

	//create index if not EXISTS xid ;
	indexTaskXid, err := client.CreateIndex(policy, TransactionManagerNamespace, BranchBarrierTable, "TXM_BARRIER_XID", "txid", as.STRING)
	dtmimp.E2P(err)
	IdxerrXid := <-indexTaskXid.OnComplete()
	dtmimp.E2P(IdxerrXid)

	//create index if not EXISTS gid on branch_barrier(gid);
	indexTask, err := client.CreateIndex(policy, TransactionManagerNamespace, BranchBarrierTable, "TXM_BARRIER_GID", "gid", as.STRING)
	dtmimp.E2P(err)

	Idxerr := <-indexTask.OnComplete()
	dtmimp.E2P(Idxerr)

	//create index if not EXISTS uniq_barrier on branch_barrier (gid,branch_id,op,barrier_id);
	indexTask2, err := client.CreateComplexIndex(policy, TransactionManagerNamespace, BranchBarrierTable, "UNIQ_BARRIER", "uniq_barrier", as.STRING, as.ICT_MAPVALUES)
	Idxerr2 := <-indexTask2.OnComplete()
	dtmimp.E2P(Idxerr2)

	now := time.Now()
	errTruncate := client.Truncate(policy, SCHEMA, BranchBarrierTable, &now)
	dtmimp.E2P(errTruncate)
}

func dropBarrierSet() {
	//drop table IF EXISTS dtm.trans_global;
	client := aerospikeGet()
	defer aerospikePut(client)

	writePolicy := &as.WritePolicy{
		RecordExistsAction: as.REPLACE,
		DurableDelete:      true,
		CommitLevel:        as.COMMIT_MASTER,
	}
	now := time.Now()
	err := client.Truncate(writePolicy, TransactionManagerNamespace, BranchBarrierTable, &now)
	dtmimp.E2P(err)

	err = client.DropIndex(writePolicy, TransactionManagerNamespace, BranchBarrierTable, "TXM_BARRIER_XID")
	dtmimp.E2P(err)

	err = client.DropIndex(writePolicy, TransactionManagerNamespace, BranchBarrierTable, "TXM_BARRIER_GID")
	dtmimp.E2P(err)

	err = client.DropIndex(writePolicy, TransactionManagerNamespace, BranchBarrierTable, "UNIQ_BARRIER")
	dtmimp.E2P(err)

}
