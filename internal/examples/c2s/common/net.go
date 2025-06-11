package common

import (
	"net"
	"time"
)

const (
	HandshakeToken   = "123"
	HandshakeTimeout = 5 * time.Second
)

const (
	PendingPacketQueueSize = 10
	MaxPacketLength        = 64 * 1024
	ReadBufSize            = 128 * 1024
	WriteBufSize           = 128 * 1024
	ReadWriteTimeout       = 60 * time.Second
	TickInterval           = 5 * time.Second
	HeartbeatTimeout       = 15 * time.Second
	InactiveTimeout        = 5 * time.Minute
)

func Dialer(addr string) (net.Conn, error) {
	return net.Dial("tcp", addr)
}

func CreateListener(addr string) (net.Listener, error) {
	return net.Listen("tcp", addr)
}
