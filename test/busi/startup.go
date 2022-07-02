package busi

import (
	"context"
	"fmt"
	"github.com/dtm-labs/dtm/dtmcli"
	"github.com/dtm-labs/dtm/dtmcli/dtmimp"
	"github.com/dtm-labs/dtm/dtmutil"
	"github.com/dtm-labs/dtm/dtmutil/aerospike/pooler"
	"github.com/gin-gonic/gin"
)

// Startup startup the busi's grpc and http service
func Startup() *gin.Engine {
	GrpcStartup()
	return BaseAppStartup()
}

var aerospikeClientPool *pooler.ASConnectionPool

// PopulateDB populate example mysql data
func PopulateDB(skipDrop bool, busiConfig dtmcli.DBConf) {

	switch busiConfig.Driver {
	case "redis":
		_, err := RedisGet().FlushAll(context.Background()).Result() // redis barrier need clear
		dtmimp.E2P(err)

		SetRedisBothAccount(10000, 10000)
	case "mongo":
		SetupMongoBarrierAndBusi()
	case "aerospike":
		asPoolConfig := &pooler.AerospikePoolConfig{
			SeedServer:      busiConfig.Host,
			UseAuth:         false,
			User:            busiConfig.User,
			Password:        busiConfig.Password,
			ConnMaxLifeTime: 0,
			MaxIdleConns:    10,
			MaxOpenConns:    20,
			InitialCapacity: 10,
		}

		cp, err := pooler.InitializeConnectionPool(asPoolConfig)
		dtmimp.E2P(err)
		aerospikeClientPool = cp
		SetAerospikeBothAccount(10000, 10000)
	default: //sql
		resetXaData()
		file := fmt.Sprintf("%s/busi.%s.sql", dtmutil.GetSQLDir(), BusiConf.Driver)
		dtmutil.RunSQLScript(BusiConf, file, skipDrop)
		file = fmt.Sprintf("%s/dtmcli.barrier.%s.sql", dtmutil.GetSQLDir(), BusiConf.Driver)
		dtmutil.RunSQLScript(BusiConf, file, skipDrop)
		file = fmt.Sprintf("%s/dtmsvr.storage.%s.sql", dtmutil.GetSQLDir(), BusiConf.Driver)
		dtmutil.RunSQLScript(BusiConf, file, skipDrop)
		//_, err := RedisGet().FlushAll(context.Background()).Result() // redis barrier need clear
		//dtmimp.E2P(err)
	}
}
