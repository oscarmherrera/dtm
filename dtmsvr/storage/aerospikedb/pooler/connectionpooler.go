package pooler

import (
	"errors"
	as "github.com/aerospike/aerospike-client-go/v5"
	"github.com/dtm-labs/dtm/dtmcli/dtmimp"
	"github.com/dtm-labs/dtm/dtmcli/logger"
	"github.com/dtm-labs/dtm/dtmsvr/config"
	"github.com/silenceper/pool"
	"strconv"
	"strings"
	"time"
)

//var authVars *utils.ASAuthentication

var connectionPool *ASConnectionPool

type ASConnectionPool struct {
	connPool pool.Pool
}

func InitializeConnectionPool(config config.Store) (*ASConnectionPool, error) {
	var auth bool = false
	if config.AerospikeAuth == "true" {
		auth = true
	}

	var UseAerospikeAuth = auth

	seedServer := config.AerospikeSeedSrv
	var asServer []string
	asServer = append(asServer, seedServer)

	node, err := ConvertIPStringToIpPort(&asServer)
	dtmimp.E2P(err)

	c, err := as.NewClient(node[0].IP.String(), node[0].Port)
	dtmimp.E2P(err)

	nodes := c.Cluster().GetNodes()
	asServers := make([]*string, len(nodes))
	for i, v := range nodes {

		host := v.GetHost().String()
		asServers[i] = &host
	}

	roundRobinASServers, err := NewRoundRobin(asServers...)
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
			policy := as.NewClientPolicy()
			policy.MinConnectionsPerNode = 50
			policy.User = config.User
			policy.Password = config.Password
			policy.Timeout = time.Duration(60 * time.Second)
			return as.NewClientWithPolicy(policy, parts[0], port)
		}

		return as.NewClient(parts[0], port)
	}

	//closeConn
	closeConn := func(v interface{}) error {
		v.(*as.Client).Close()
		return nil
	}
	timeout := int(config.ConnMaxLifeTime)
	poolConfig := &pool.Config{
		InitialCap:  20,
		MaxIdle:     int(config.MaxIdleConns) + 10,
		MaxCap:      int(config.MaxOpenConns) + 10,
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
	connectionPool = asConnPool
	return asConnPool, nil

}

func GetConnectionPool() *ASConnectionPool {
	return connectionPool
}

func (p *ASConnectionPool) Get() (interface{}, error) {
	conn, err := p.connPool.Get()
	//logger.Debugf("connection get, pool depth:%d", p.PoolDepth())
	return conn, err
}

func (p *ASConnectionPool) Put(c interface{}) {
	err := p.connPool.Put(c)
	if err != nil {
		logger.Errorf("connection pool put")
		return
	}
	//logger.Debugf("connection put, pool depth:%d", p.PoolDepth())
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
