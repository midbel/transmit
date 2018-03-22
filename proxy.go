package transmit

import (
	"crypto/tls"
	"io"
	"io/ioutil"
	"net"
	"sync"
	"time"
)

type proxy struct {
	net.Conn

	addr string
	cert *tls.Config

	mu     sync.Mutex
	writer io.Writer
}

func Proxy(a string, c *tls.Config) (net.Conn, error) {
	s, err := net.Dial("tcp", a)
	if err != nil {
		return nil, err
	}
	if c != nil {
		s = tls.Client(s, c)
	}
	return &proxy{Conn: s, writer: s, cert: c, addr: a}, nil
}

func (p *proxy) Write(bs []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	_, err := p.writer.Write(bs)
	if err != nil {
		Logger.Printf("fail to write %d bytes to %s: %s", len(bs), p.addr, err)
	}
	if e, ok := err.(net.Error); ok && !e.Temporary() {
		p.Conn.Close()
		p.writer, p.Conn = ioutil.Discard, nil

		go p.redial()
		err = nil
	}
	return len(bs), nil
}

func (p *proxy) redial() {
	var err error
	for i := 0; ; i++ {
		p.Conn, err = net.DialTimeout("tcp", p.addr, time.Second*5)
		if err == nil {
			break
		}
		s := (i % 5) + 1
		time.Sleep(time.Second * time.Duration(s))
	}
	if p.cert != nil {
		p.Conn = tls.Client(p.Conn, p.cert)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.writer = p.Conn
}
