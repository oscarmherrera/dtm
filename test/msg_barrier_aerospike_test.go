package test

import (
	"errors"
	"github.com/dtm-labs/dtm/dtmcli"
	"github.com/dtm-labs/dtm/dtmcli/dtmimp"
	"github.com/dtm-labs/dtm/test/busi"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMsgAerospikeDoSucceed(t *testing.T) {
	before := getBeforeBalances("aerospike")
	gid := dtmimp.GetFuncName()
	req := busi.GenTransReq(30, false, false)
	msg := dtmcli.NewMsg(DtmServer, gid).
		Add(busi.Busi+"/SagaAerospikeTransIn", req)
	err := msg.DoAndSubmit(Busi+"/AerospikeQueryPrepared", func(bb *dtmcli.BranchBarrier) error {
		client := busi.AerospikeGet()
		defer busi.AerospikePut(client)
		return bb.AerospikeCall(client, func() error {
			c := busi.AerospikeGet()
			defer busi.AerospikePut(c)
			return busi.SagaAerospikeAdjustBalance(c, busi.TransOutUID, -30, "")
		})
	})
	assert.Nil(t, err)
	waitTransProcessed(msg.Gid)
	assert.Equal(t, []string{StatusSucceed}, getBranchesStatus(msg.Gid))
	assert.Equal(t, StatusSucceed, getTransStatus(msg.Gid))
	assertNotSameBalance(t, before, "aerospike")
}

func TestMsgAerospikeDoBusiFailed(t *testing.T) {
	before := getBeforeBalances("aerospike")
	gid := dtmimp.GetFuncName()
	req := busi.GenTransReq(30, false, false)
	msg := dtmcli.NewMsg(DtmServer, gid).Add(busi.Busi+"/SagaAerospikeTransIn", req)
	err := msg.DoAndSubmit(Busi+"/AerospikeQueryPrepared", func(bb *dtmcli.BranchBarrier) error {
		return errors.New("an error")
	})
	assert.Error(t, err)
	assertSameBalance(t, before, "aerospike")
}

func TestMsgAerospikeDoBusiLater(t *testing.T) {
	before := getBeforeBalances("aerospike")
	gid := dtmimp.GetFuncName()
	req := busi.GenTransReq(30, false, false)
	_, err := dtmcli.GetRestyClient().R().
		SetQueryParams(map[string]string{
			"trans_type": "msg",
			"gid":        gid,
			"branch_id":  dtmimp.MsgDoBranch0,
			"op":         dtmimp.MsgDoOp,
			"barrier_id": dtmimp.MsgDoBarrier1,
		}).
		SetBody(req).Get(Busi + "/AerospikeQueryPrepared")
	assert.Nil(t, err)
	msg := dtmcli.NewMsg(DtmServer, gid).
		Add(busi.Busi+"/SagaAerospikeTransIn", req)

	err = msg.DoAndSubmit(Busi+"/AerospikeQueryPrepared", func(bb *dtmcli.BranchBarrier) error {
		client := busi.AerospikeGet()
		defer busi.AerospikePut(client)
		return bb.AerospikeCall(client, func() error {
			c := busi.AerospikeGet()
			defer busi.AerospikePut(c)
			return busi.SagaAerospikeAdjustBalance(c, busi.TransOutUID, -30, "")
		})
	})

	assert.Error(t, err, dtmcli.ErrDuplicated)
	assertSameBalance(t, before, "aerospike")
}

func TestMsgAerospikeDoCommitFailed(t *testing.T) {
	before := getBeforeBalances("aerospike")
	gid := dtmimp.GetFuncName()
	req := busi.GenTransReq(30, false, false)
	msg := dtmcli.NewMsg(DtmServer, gid).
		Add(busi.Busi+"/SagaAerospikeTransIn", req)
	err := msg.DoAndSubmit(Busi+"/AerospikeQueryPrepared", func(bb *dtmcli.BranchBarrier) error {
		return errors.New("after commit error")
	})
	assert.Error(t, err)
	assertSameBalance(t, before, "aerospike")

}

func TestMsgAerospikeDoCommitAfterFailed(t *testing.T) {
	before := getBeforeBalances("aerospike")
	gid := dtmimp.GetFuncName()
	req := busi.GenTransReq(30, false, false)
	msg := dtmcli.NewMsg(DtmServer, gid).
		Add(busi.Busi+"/SagaAerospikeTransIn", req)

	err := msg.DoAndSubmit(Busi+"/AerospikeQueryPrepared", func(bb *dtmcli.BranchBarrier) error {
		client := busi.AerospikeGet()
		defer busi.AerospikePut(client)
		return bb.AerospikeCall(client, func() error {
			c := busi.AerospikeGet()
			defer busi.AerospikePut(c)
			err := busi.SagaAerospikeAdjustBalance(c, busi.TransOutUID, -30, "")
			dtmimp.E2P(err)
			return errors.New("an error")
		})
	})

	//err := msg.DoAndSubmit(Busi+"/AerospikeQueryPrepared", func(bb *dtmcli.BranchBarrier) error {
	//	c := busi.AerospikeGet()
	//	defer busi.AerospikePut(c)
	//	err := busi.SagaAerospikeAdjustBalance(c, busi.TransOutUID, -30, "")
	//	dtmimp.E2P(err)
	//	return errors.New("an error")
	//})
	assert.Error(t, err)
	waitTransProcessed(gid)
	assertNotSameBalance(t, before, "aerospike")
}
