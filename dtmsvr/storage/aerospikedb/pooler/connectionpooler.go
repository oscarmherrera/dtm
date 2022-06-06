package pooler

import (
	"errors"
	"github.com/aerospike/aerospike-client-go/v5"
	"github.com/dtm-labs/dtm/dtmcli/logger"
	"github.com/dtm-labs/dtm/dtmsvr/config"
	"github.com/silenceper/pool"
	"strconv"
	"strings"
	"time"
)

//var authVars *utils.ASAuthentication

type ASConnectionPool struct {
	connPool pool.Pool
}

//var roundRobinASServers utils.RoundRobin

func InitializeConnectionPool(config config.Store) (*ASConnectionPool, error) {
	//factory
	//servers = *asServers
	//serversCount = len(servers)

	var UseAerospikeAuth = true

	asServers := []string{"one:port", "two:port", "three:port"} // Need to go and get all the aerospike servers from a seed server
	var servers []*string
	for _, server := range asServers {
		s := server
		servers = append(servers, &s)
	}

	roundRobinASServers, err := NewRoundRobin(servers...)
	if err != nil {
		logger.Errorf(err.Error())
		return nil, err
	}
	factory := func() (interface{}, error) {
		nextServer := *roundRobinASServers.Next()
		parts := strings.Split(nextServer, ":")
		if len(parts) <= 1 {
			logger.FatalIfError(errors.New("invalid server address it shoud be <ip:port>"))
		}
		port, err := strconv.Atoi(parts[1])
		if err != nil {
			logger.FatalIfError(errors.New("invalid port provide it shoud be <ip:port>"))
		}
		if UseAerospikeAuth {

			//authVars = config.ASAuth
			policy := aerospike.NewClientPolicy()
			policy.MinConnectionsPerNode = 50
			policy.User = config.User
			policy.Password = config.Password
			policy.Timeout = time.Duration(60 * time.Second)
			return aerospike.NewClientWithPolicy(policy, parts[0], port)
		}

		return aerospike.NewClient(parts[0], port)
	}

	//closeConn
	closeConn := func(v interface{}) error {
		v.(*aerospike.Client).Close()
		return nil
	}
	timeout := int(config.ConnMaxLifeTime)
	poolConfig := &pool.Config{
		InitialCap:  5,
		MaxIdle:     int(config.MaxIdleConns),
		MaxCap:      int(config.MaxOpenConns),
		Factory:     factory,
		Close:       closeConn,
		IdleTimeout: time.Duration(time.Second * time.Duration(timeout)),
	}
	p, err := pool.NewChannelPool(poolConfig)
	if err != nil {
		return nil, err
	}

	asConnPool := &ASConnectionPool{
		connPool: p,
	}

	return asConnPool, nil

}

func (p *ASConnectionPool) Get() (interface{}, error) {
	return p.connPool.Get()
}

func (p *ASConnectionPool) Put(c interface{}) {
	err := p.connPool.Put(c)
	if err != nil {
		logger.Errorf("connection pool put")
		return
	}
}

func (p *ASConnectionPool) Release() {
	p.connPool.Release()
}

func (p *ASConnectionPool) PoolDepth() int {
	return p.connPool.Len()
}

//func (p *ASConnectionPool) GetUserName() string {
//	return authVars.UserName
//}

//func (p *ASConnectionPool) GetPassword() string {
//	return authVars.Password
//}
