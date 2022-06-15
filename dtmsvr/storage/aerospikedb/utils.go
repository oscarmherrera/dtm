package aerospikedb

import (
	"github.com/dtm-labs/dtm/dtmcli/dtmimp"
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
