package aerospikedb

import (
	"errors"
	"fmt"
	as "github.com/aerospike/aerospike-client-go/v5"
	"github.com/dtm-labs/dtm/dtmcli/dtmimp"
	"github.com/dtm-labs/dtm/dtmcli/logger"
	"github.com/rs/xid"
	"go.uber.org/zap"
	"net"
	"strconv"
	"strings"
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

	xid, err := xid.FromBytes(xidValue)
	dtmimp.E2P(err)

	var timeOut time.Time
	timeOut.GobDecode(timeOutValue)

	gTxId := &globalTxId{
		xid:     xid,
		timeOut: timeOut,
	}

	return gTxId, nil
}

func GetConnection(asdb *Store) (*as.Client, error) {
	var connected bool
	var client *as.Client

	connected = false

	for _, node := range *asdb.endpoints {

		c, err := as.NewClient(node.IP.String(), node.Port)
		if err != nil {
			logger.Warnf("error connecting to node: %s", err)
		} else {
			if c.IsConnected() {
				logger.Infof("connected to node IP: %s, Port: %d", node.IP.String(), node.Port)
				connected = true
				client = c
				break
			}
		}
	}

	if !connected {
		return nil, errors.New("unable to connected to any aerospike database server")
	}

	return client, nil
}

type Node struct {
	net.IP
	Port int
}

func (n Node) String() string {
	return fmt.Sprintf("%s:%d", n.IP.String(), n.Port)
}

func ConvertIPStringToIpPort(logger *zap.Logger, ipStrs *[]string) (*[]Node, error) {
	var nodes []Node

	for _, ipString := range *ipStrs {
		var node Node

		ipInfo := strings.Split(ipString, ":")
		ipAddr := net.ParseIP(ipInfo[0])
		node.IP = ipAddr
		if len(ipInfo) == 1 {
			//We are going to use the default port
			node.Port = 3000
		} else {
			port, err := strconv.Atoi(ipInfo[1])
			if err != nil {
				logger.Error("error invalid ip address port format", zap.Error(err), zap.String("ip_string", ipString))
				continue
			}
			node.Port = port
		}
		nodes = append(nodes, node)

	}
	if len(nodes) == 0 {
		return nil, errors.New("no valid array of node ips were configures '<ip addr:port>'")
	}

	return &nodes, nil
}
