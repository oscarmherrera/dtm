/*
 * Copyright (c) 2021 yedf. All rights reserved.
 * Use of this source code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */

package test

import (
	"testing"

	"github.com/dtm-labs/dtm/dtmcli"
	"github.com/dtm-labs/dtm/dtmcli/dtmimp"
	"github.com/dtm-labs/dtm/test/busi"
	"github.com/stretchr/testify/assert"
)

func TestSagaBarrierAerospikeNormal(t *testing.T) {
	busi.SetAerospikeBothAccount(100, 100)
	before := getBeforeBalances("aerospike")
	saga := genSagaBarrierAerospike(dtmimp.GetFuncName())
	err := saga.Submit()
	assert.Nil(t, err)
	waitTransProcessed(saga.Gid)
	assert.Equal(t, []string{StatusPrepared, StatusSucceed, StatusPrepared, StatusSucceed}, getBranchesStatus(saga.Gid))
	assert.Equal(t, StatusSucceed, getTransStatus(saga.Gid))
	assertNotSameBalance(t, before, "aerospike")
}

func TestSagaBarrierAerospikeRollback(t *testing.T) {
	busi.SetAerospikeBothAccount(20, 20)
	before := getBeforeBalances("aerospike")
	saga := genSagaBarrierAerospike(dtmimp.GetFuncName())
	err := saga.Submit()
	assert.Nil(t, err)
	waitTransProcessed(saga.Gid)
	assert.Equal(t, StatusFailed, getTransStatus(saga.Gid))
	assert.Equal(t, []string{StatusSucceed, StatusSucceed, StatusSucceed, StatusFailed}, getBranchesStatus(saga.Gid))
	assertSameBalance(t, before, "aerospike")
}

func genSagaBarrierAerospike(gid string) *dtmcli.Saga {
	req := busi.GenReqHTTP(30, false, false)
	req.Store = "aerospike"
	return dtmcli.NewSaga(DtmServer, gid).
		Add(Busi+"/SagaAerospikeTransIn", Busi+"/SagaAerospikeTransInCom", req).
		Add(Busi+"/SagaAerospikeTransOut", Busi+"/SagaAerospikeTransOutCom", req)
}
