/*
 * Copyright (c) 2021 yedf. All rights reserved.
 * Use of this source code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */

package test

import (
	"os"
	"testing"
	"time"

	"github.com/dtm-labs/dtm/dtmcli"
	"github.com/dtm-labs/dtm/dtmcli/logger"
	"github.com/dtm-labs/dtm/dtmgrpc"
	"github.com/dtm-labs/dtm/dtmsvr"
	"github.com/dtm-labs/dtm/dtmsvr/config"
	"github.com/dtm-labs/dtm/dtmsvr/storage/registry"
	"github.com/dtm-labs/dtm/test/busi"
	"github.com/go-resty/resty/v2"
)

func exitIf(code int) {
	if code != 0 {
		os.Exit(code)
	}
}

func TestMain(m *testing.M) {
	config.MustLoadConfig("../test.yml")
	logger.InitLog("debug")
	dtmcli.SetCurrentDBType(busi.BusiConf.Driver)
	dtmsvr.TransProcessedTestChan = make(chan string, 1)
	dtmsvr.NowForwardDuration = 0 * time.Second
	dtmsvr.CronForwardDuration = 180 * time.Second
	conf.UpdateBranchSync = 1

	dtmgrpc.AddUnaryInterceptor(busi.SetGrpcHeaderForHeadersYes)
	dtmcli.GetRestyClient().OnBeforeRequest(busi.SetHTTPHeaderForHeadersYes)
	dtmcli.GetRestyClient().OnAfterResponse(func(c *resty.Client, resp *resty.Response) error { return nil })

	tenv := os.Getenv("TEST_STORE")
	switch tenv {
	case "boltdb":
		conf.Store.Driver = "boltdb"
	case "mysql":
		conf.Store.Driver = "mysql"
		conf.Store.Host = "localhost"
		conf.Store.Port = 3306
		conf.Store.User = "root"
		conf.Store.Password = ""
	case "aerospike":
		conf.Store.Driver = "aerospike"
		conf.Store.Host = "localhost"
		conf.Store.User = "admin"
		conf.Store.Password = "admin"
		conf.Store.Port = 3000
		conf.Store.MaxOpenConns = 50
		conf.Store.MaxIdleConns = 20
		conf.Store.AerospikeNamespace = "test"
		conf.Store.AerospikeSeedSrv = "10.211.55.200:3000"
	default:
		conf.Store.Driver = "redis"
		conf.Store.Host = "localhost"
		conf.Store.User = ""
		conf.Store.Password = ""
		conf.Store.Port = 6379
	}

	//if tenv == "boltdb" {
	//	conf.Store.Driver = "boltdb"
	//} else if tenv == "mysql" {
	//	conf.Store.Driver = "mysql"
	//	conf.Store.Host = "localhost"
	//	conf.Store.Port = 3306
	//	conf.Store.User = "root"
	//	conf.Store.Password = ""
	//} else {
	//	conf.Store.Driver = "redis"
	//	conf.Store.Host = "localhost"
	//	conf.Store.User = ""
	//	conf.Store.Password = ""
	//	conf.Store.Port = 6379
	//}
	registry.WaitStoreUp()

	dtmsvr.PopulateDB(false)
	//dtmsvr.PopulateDB(true)
	go dtmsvr.StartSvr()

	busi.PopulateDB(false, "aerospike")
	_ = busi.Startup()
	r := m.Run()
	exitIf(r)
	close(dtmsvr.TransProcessedTestChan)
	gid, more := <-dtmsvr.TransProcessedTestChan
	logger.FatalfIf(more, "extra gid: %s in test chan", gid)
	os.Exit(0)
}
