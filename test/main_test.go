/*
 * Copyright (c) 2021 yedf. All rights reserved.
 * Use of this source code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */
package test

import (
	"github.com/dtm-labs/dtm/dtmcli"
	"github.com/dtm-labs/dtm/dtmcli/logger"
	"github.com/dtm-labs/dtm/dtmgrpc"
	"github.com/dtm-labs/dtm/dtmsvr"
	"github.com/dtm-labs/dtm/dtmsvr/config"
	"github.com/dtm-labs/dtm/dtmsvr/storage/registry"
	"github.com/dtm-labs/dtm/test/busi"
	"github.com/go-resty/resty/v2"
	"os"
	"testing"
	"time"
)

func exitIf(code int) {
	if code != 0 {
		os.Exit(code)
	}
}
func TestMain(m *testing.M) {
	config.MustLoadConfig("../test.yml")
	//logger.InitLog("info")
	logger.InitLog("debug")
	dtmsvr.TransProcessedTestChan = make(chan string, 1)
	dtmsvr.NowForwardDuration = 0 * time.Second
	dtmsvr.CronForwardDuration = 180 * time.Second
	conf.UpdateBranchSync = 1
	dtmgrpc.AddUnaryInterceptor(busi.SetGrpcHeaderForHeadersYes)
	dtmcli.GetRestyClient().OnBeforeRequest(busi.SetHTTPHeaderForHeadersYes)
	dtmcli.GetRestyClient().OnAfterResponse(func(c *resty.Client, resp *resty.Response) error { return nil })

	tenv := os.Getenv("TEST_STORE")
	if conf.Store.Driver == "" {
		// only do this if the test.yml is empty
		conf.Store.Driver = tenv
	}

	conf.Store.Host = "localhost"
	conf.Store.Db = ""
	switch conf.Store.Driver {
	case "redis":
		conf.Store.Driver = "redis"
		conf.Store.User = ""
		conf.Store.Password = ""
		conf.Store.Port = 6379

		busi.BusiConf.Driver = conf.Store.Driver
		busi.BusiConf.User = conf.Store.User
		busi.BusiConf.Password = conf.Store.Password
		busi.BusiConf.Port = conf.Store.Port
		busi.BusiConf.Host = conf.Store.Host
	case "boltdb":
		conf.Store.Driver = "boltdb"
	case "mysql":
		conf.Store.Port = 3306
		conf.Store.User = "root"
		conf.Store.Password = ""
	case "aerospike":
		conf.Store.User = "admin"
		conf.Store.Password = "admin"
		conf.Store.Port = 3000
		conf.Store.MaxOpenConns = 50
		conf.Store.MaxIdleConns = 20
		conf.Store.AerospikeNamespace = "test"
		conf.Store.AerospikeSeedSrv = "10.211.55.200:3000"

		busi.BusiConf.Driver = config.Aerospike
		busi.BusiConf.User = conf.Store.User
		busi.BusiConf.Password = conf.Store.Password
		busi.BusiConf.Port = conf.Store.Port
		busi.BusiConf.Host = "10.211.55.200"

	case "postgres":
		conf.Store.Host = "localhost"
		conf.Store.Port = 5432
		conf.Store.User = "postgres"
		conf.Store.Password = "postgres"
		conf.Store.Db = "dtm"

	default:
		conf.Store.User = ""
		conf.Store.Password = ""
		conf.Store.Port = 6379
	}

	registry.WaitStoreUp()
	dtmsvr.PopulateDB(false)
	conf.Store.Db = "dtm" // after populateDB, set current db to dtm
	if tenv == "postgres" {
		busi.BusiConf = conf.Store.GetDBConf()
		dtmcli.SetCurrentDBType(tenv)
	}
	go dtmsvr.StartSvr()

	busi.PopulateDB(false, busi.BusiConf)

	_ = busi.Startup()
	r := m.Run()
	exitIf(r)
	close(dtmsvr.TransProcessedTestChan)
	gid, more := <-dtmsvr.TransProcessedTestChan
	logger.FatalfIf(more, "extra gid: %s in test chan", gid)
	os.Exit(0)
}
