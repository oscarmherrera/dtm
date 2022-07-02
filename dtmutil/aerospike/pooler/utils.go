package pooler

import (
	"errors"
	"fmt"
	"github.com/dtm-labs/dtm/dtmcli/logger"
	"net"
	"strconv"
	"strings"
)

type Node struct {
	net.IP
	Port int
}

func (n Node) String() string {
	return fmt.Sprintf("%s:%d", n.IP.String(), n.Port)
}

func ConvertIPStringToIpPort(ipStrs *[]string) ([]Node, error) {
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
				logger.Errorf("error invalid ip address port format: %v", err)
				continue
			}
			node.Port = port
		}
		nodes = append(nodes, node)

	}
	if len(nodes) == 0 {
		return nil, errors.New("no valid array of node ips were configures '<ip addr:port>'")
	}

	return nodes, nil
}
