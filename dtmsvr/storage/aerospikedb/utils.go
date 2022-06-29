package aerospikedb

import (
	as "github.com/aerospike/aerospike-client-go/v5"
	"github.com/dtm-labs/dtm/dtmcli/dtmimp"
	"github.com/dtm-labs/dtm/dtmsvr/storage"
	"github.com/dtm-labs/dtm/dtmutil"
	"github.com/rs/xid"
	"time"
)

type globalTxId struct {
	xid     xid.ID
	timeOut time.Time
}

func EncodeKey(guid xid.ID, timeOut time.Duration) ([]byte, time.Time) {
	timeout := time.Now().Local().Add(time.Duration((timeOut * time.Millisecond)))

	timeOutinBytes, errTimeout := timeout.GobEncode()
	dtmimp.E2P(errTimeout)

	keyValue := make([]byte, 20)

	keyValue = append(keyValue, guid.Bytes()...)
	keyValue = append(keyValue, timeOutinBytes...)

	return keyValue, timeout
}

func DecodeKey(keyValue []byte) (*globalTxId, error) {
	xidValue := make([]byte, 12)
	timeOutValue := make([]byte, 8)

	copy(xidValue, keyValue[0:11])
	copy(timeOutValue, keyValue[12:19])

	txid, err := xid.FromBytes(xidValue)
	dtmimp.E2P(err)

	var timeOut time.Time
	err = timeOut.GobDecode(timeOutValue)
	if err != nil {
		return nil, err
	}

	gTxId := &globalTxId{
		xid:     txid,
		timeOut: timeOut,
	}

	return gTxId, nil
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

	if asRecord.Bins["ext_data"] != nil {
		tranRecord.ExtData = asRecord.Bins["ext_data"].(string)
	}

	if asRecord.Bins["rollback_reason"] != nil {
		tranRecord.RollbackReason = asRecord.Bins["rollback_reason"].(string)
	}

	if asRecord.Bins["owner"] != nil {
		tranRecord.Owner = asRecord.Bins["owner"].(string)
	}

	return tranRecord
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
