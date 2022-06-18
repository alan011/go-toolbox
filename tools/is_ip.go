package tools

import (
	"net"
)

func IsIPv4(ipv4 string) bool {
	return net.ParseIP(ipv4) != nil
}
