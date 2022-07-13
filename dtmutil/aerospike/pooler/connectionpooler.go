package pooler

import (
	as "github.com/aerospike/aerospike-client-go/v5"
	"github.com/dtm-labs/dtm/dtmcli/dtmimp"
	"github.com/dtm-labs/dtm/dtmcli/logger"
	"github.com/silenceper/pool"
	"sync"
	"time"
)

var connectionPool *ASConnectionPool

type ASConnectionPool struct {
	connPool pool.Pool
}

type AerospikePoolConfig struct {
	SeedServer      string
	UseAuth         bool
	User            string
	Password        string
	ConnMaxLifeTime int
	MaxIdleConns    int
	MaxOpenConns    int
	InitialCapacity int
}

func InitializeConnectionPool(config *AerospikePoolConfig) (*ASConnectionPool, error) {

	var UseAerospikeAuth = config.UseAuth

	if config.ConnMaxLifeTime == 0 {
		config.ConnMaxLifeTime = 10 //seconds
	}

	if config.InitialCapacity == 0 {
		config.InitialCapacity = 20
	}

	seedServer := config.SeedServer
	var asServer []string
	asServer = append(asServer, seedServer)

	node, err := convertStringToIPNode(seedServer)
	dtmimp.E2P(err)

	if err != nil {
		logger.Errorf(err.Error())
		return nil, err
	}
	factory := func() (interface{}, error) {

		policy := as.NewClientPolicy()
		policy.MinConnectionsPerNode = 1
		policy.Timeout = time.Duration(60 * time.Second)

		if UseAerospikeAuth {

			//authVars = config.ASAuth
			policy.User = config.User
			policy.Password = config.Password
			logger.Debugf("connection factory: new connection with policy to server(%s) port(%d)", node.IP.String(), node.Port)
			return as.NewClientWithPolicy(policy, node.IP.String(), node.Port)
		}
		return as.NewClientWithPolicy(policy, node.IP.String(), node.Port)
	}

	//closeConn
	closeConn := func(v interface{}) error {
		v.(*as.Client).Close()
		return nil
	}
	timeout := config.ConnMaxLifeTime
	poolConfig := &pool.Config{
		InitialCap:  config.InitialCapacity,
		MaxIdle:     config.MaxIdleConns + 5,
		MaxCap:      config.MaxOpenConns + 5,
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

func getConnectionPool() *ASConnectionPool {
	return connectionPool
}

func (p *ASConnectionPool) PoolPut(c interface{}) {
	err := p.connPool.Put(c)
	if err != nil {
		logger.Errorf("connection pool put")
		return
	}
}

func (p *ASConnectionPool) PoolGet() (interface{}, error) {
	conn, err := p.connPool.Get()
	if err != nil {
		dtmimp.E2P(err)
	}
	return conn, err
}

func (p *ASConnectionPool) Get() (interface{}, error) {
	var wg sync.WaitGroup

	clientChannel := make(chan *as.Client, 1)
	errorChannel := make(chan error, 1)

	wg.Add(1)
	go func(wg *sync.WaitGroup, c chan *as.Client, e chan error) {
		defer wg.Done()
		seedserver := "10.211.55.200"
		policy := as.NewClientPolicy()
		policy.MinConnectionsPerNode = 5
		//policy.User = config.User
		//policy.Password = config.Password
		policy.Timeout = time.Duration(300 * time.Second)

		client, err := as.NewClientWithPolicy(policy, seedserver, 3000)
		if err != nil {
			c <- nil
			e <- err
			return
		}
		c <- client
		e <- nil
		return
	}(&wg, clientChannel, errorChannel)

	wg.Wait()
	client := <-clientChannel
	err := <-errorChannel
	return client, err
}

func (p *ASConnectionPool) Put(c interface{}) {
	c.(*as.Client).Close()
}

func (p *ASConnectionPool) Release() {
	p.connPool.Release()
}

func (p *ASConnectionPool) PoolDepth() int {
	return p.connPool.Len()
}
