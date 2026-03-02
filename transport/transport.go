package transport

import (
	"net"
	"net/http"
	"net/rpc"
)

type Transport interface {
	Call(
		peer int,
		svcMeth string,
		args interface{},
		reply interface{},
	) bool
	Start() error
	Close() error
}

type HTTPTransport struct {
	selfID     int
	listenAddr string
	peers      map[int]string
	rpcServer  *rpc.Server
	listener   net.Listener
}

func (t *HTTPTransport) Register(rcvr interface{}) error {
	return t.rpcServer.Register(rcvr)
}

func NewHTTPTransport(
	selfID int,
	listenAddr string,
	peers map[int]string,
) *HTTPTransport {
	return &HTTPTransport{
		selfID:     selfID,
		listenAddr: listenAddr,
		peers:      peers,
		rpcServer:  rpc.NewServer(),
	}
}

func (t *HTTPTransport) Start() error {
	mux := http.NewServeMux()
	mux.Handle(rpc.DefaultRPCPath, t.rpcServer)

	ln, err := net.Listen("tcp", t.listenAddr)
	if err != nil {
		return err
	}
	t.listener = ln

	go func() {
		_ = http.Serve(ln, mux)
	}()
	return nil
}

func (t *HTTPTransport) Call(
	peer int,
	svcMeth string,
	args interface{},
	reply interface{},
) bool {
	addr, ok := t.peers[peer]
	if !ok {
		return false
	}

	client, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		return false
	}
	defer client.Close()

	if err := client.Call(svcMeth, args, reply); err != nil {
		return false
	}
	return true
}

func (t *HTTPTransport) Close() error {
	if t.listener != nil {
		return t.listener.Close()
	}
	return nil
}
