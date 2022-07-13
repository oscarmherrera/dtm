package dtmcli

import (
	"errors"
	"fmt"
	"github.com/aerospike/aerospike-client-go/v5"
	"github.com/dtm-labs/dtm/dtmcli/dtmimp"
	"github.com/dtm-labs/dtm/dtmcli/logger"
	"github.com/rs/xid"
	"time"
)

var TransactionManagerNamespace = "test"
var BranchBarrierTable = "branch_barrier"

// AerospikeCall sub-trans barrier for aerospike. see http://dtm.pub/practice/barrier
// experimental
//func (bb *BranchBarrier) AerospikeCall(c *aerospike.Client, busiCall func(c *aerospike.Client) error) (rerr error) {
func (bb *BranchBarrier) AerospikeCall(c *aerospike.Client, busiCall func() error) (rerr error) {
	bid := bb.newBarrierID()
	return func() (rerr error) {

		originOp := map[string]string{
			dtmimp.OpCancel:     dtmimp.OpTry,
			dtmimp.OpCompensate: dtmimp.OpAction,
		}[bb.Op]

		originAffected, oerr := aerospikeInsertBarrier(c, bb.TransType, bb.Gid, bb.BranchID, originOp, bid, bb.Op)
		currentAffected, rerr := aerospikeInsertBarrier(c, bb.TransType, bb.Gid, bb.BranchID, bb.Op, bid, bb.Op)
		logger.Debugf("AerospikeCall: originAffected: %d currentAffected: %d", originAffected, currentAffected)

		if rerr == nil && bb.Op == dtmimp.MsgDoOp && currentAffected == 0 { // for msg's DoAndSubmit, repeated insert should be rejected.
			return ErrDuplicated
		}

		if rerr == nil {
			rerr = oerr
		}
		if (bb.Op == dtmimp.OpCancel || bb.Op == dtmimp.OpCompensate) && originAffected > 0 || // null compensate
			currentAffected == 0 { // repeated request or dangled request
			return
		}
		if rerr == nil {
			rerr = busiCall()
		}
		return
	}()
}

// AerospikeQueryPrepared query prepared for Aerospike
// experimental
func (bb *BranchBarrier) AerospikeQueryPrepared(c *aerospike.Client) error {
	_, err := aerospikeInsertBarrier(c, bb.TransType, bb.Gid, dtmimp.MsgDoBranch0, dtmimp.MsgDoOp, dtmimp.MsgDoBarrier1, dtmimp.OpRollback)
	var result *aerospike.Record
	if err == nil {
		result, err = aerospikeGetBarrier(c, bb.Gid, dtmimp.MsgDoBranch0, dtmimp.MsgDoOp, dtmimp.MsgDoBarrier1)
		if err != nil {
			logger.Errorf("AerospikeQueryPrepared: %s", err)
			return err
		}

	}

	var reason string
	if err == nil {
		reason, _ = result.Bins["reason"].(string)
	}

	if err == nil && reason == dtmimp.OpRollback {
		return ErrFailure
	}
	return err
}

func aerospikeInsertBarrier(c *aerospike.Client, transType string, gid string, branchID string, op string, barrierID string, reason string) (int64, error) {
	if op == "" {
		return 0, nil
	}

	record, err := aerospikeGetBarrier(c, gid, branchID, op, barrierID)
	if err != nil {
		if err.Error() == "NOT_FOUND" {
			uniqBarrier := map[string]interface{}{
				"gid":        gid,
				"branch_id":  branchID,
				"op":         op,
				"barrier_id": barrierID,
			}
			keyString := fmt.Sprintf("%s:%s:%s:%s", gid, branchID, op, barrierID)
			key, err := aerospike.NewKey(TransactionManagerNamespace, BranchBarrierTable, keyString)

			dtmimp.E2P(err)
			now := time.Now()
			bins := aerospike.BinMap{}
			bins["txid"] = xid.New().Bytes()
			bins["trans_type"] = transType
			bins["gid"] = gid
			bins["branch_id"] = branchID
			bins["op"] = op
			bins["barrier_id"] = barrierID
			bins["reason"] = reason
			bins["create_time"] = now.UnixNano()
			bins["update_time"] = now.UnixNano()
			bins["uniq_barrier"] = uniqBarrier

			policy := aerospike.NewWritePolicy(0, 0)
			policy.CommitLevel = aerospike.COMMIT_ALL
			policy.TotalTimeout = 200 * time.Millisecond

			err = c.Put(policy, key, bins)
			if err != nil {
				return 0, err
			}
			logger.Debugf("aerospikeInsertBarrier: created record %v", bins)
			return 1, err
		}
	}
	logger.Debugf("aerospikeInsertBarrier: found record %v", record.Bins)
	return 0, err
}

func aerospikeGetBarrier(client *aerospike.Client, gid string, branch_id string, op string, barrier_id string) (*aerospike.Record, error) {
	logger.Debugf("aerospikeGetBarrier: getting barrier gid(%s), branch_id(%s), op(%s), barrier_id (%s)", gid, branch_id, op, barrier_id)
	policy := &aerospike.BasePolicy{}

	keyString := fmt.Sprintf("%s:%s:%s:%s", gid, branch_id, op, barrier_id)
	key, err := aerospike.NewKey(TransactionManagerNamespace, BranchBarrierTable, keyString)

	if err != nil {
		logger.Errorf("CheckTransGlobalTableForGIDExists: %s", err)
		return nil, err
	}

	var bins = getBarrierBins()
	//binString := strings.Join(*bins, ",")
	record, err := client.Get(policy, key, *bins...)
	if err != nil {
		logger.Errorf("aerospikeGetBarrier: get error, %s", err.Error())
		if err.Matches(aerospike.ErrKeyNotFound.ResultCode) == true {
			return nil, errors.New("NOT_FOUND")
		}

		return nil, err
	}
	logger.Debugf("aerospikeGetBarrier: found record, %v", record.Bins)

	return record, nil

}

func convertAerospikeRecordToBarrier(asRecord *aerospike.Record) *BranchBarrier {

	barrier := &BranchBarrier{
		TransType:        asRecord.Bins["trans_type"].(string),
		Gid:              asRecord.Bins["gid"].(string),
		BranchID:         asRecord.Bins["branch_id"].(string),
		Op:               asRecord.Bins["op"].(string),
		BarrierID:        asRecord.Bins["barrier_id"].(int),
		DBType:           dtmimp.DBTypeAerospike,
		BarrierTableName: dtmimp.BarrierTableName,
	}
	return barrier
}

func getBarrierBins() *[]string {

	binList := []string{
		"txid",
		"trans_type",
		"gid",
		"branch_id",
		"op",
		"barrier_id",
		"reason",
		"create_time",
		"update_time",
		"uniq_barrier",
	}
	return &binList
}
