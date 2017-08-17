package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/midbel/transmit"
)

var ErrDone = errors.New("done")

type Config struct {
	Listen  bool             `json:"-"`
	Quiet   bool             `json:"-"`
	Retry   int              `json:"-"`
	Address string           `json:"gateway"`
	Proxy   string           `json:"proxy"`
	Routes  []transmit.Route `json:"routes"`
}

func main() {
	config := new(Config)
	flag.IntVar(&config.Retry, "r", 0, "retry")
	flag.BoolVar(&config.Listen, "l", config.Listen, "listen")
	flag.BoolVar(&config.Quiet, "q", config.Quiet, "quiet")
	flag.Parse()

	f, err := os.Open(flag.Arg(0))
	if err != nil {
		log.Fatalln(err)
	}
	if err := json.NewDecoder(f).Decode(&config); err != nil {
		log.Fatalln(err)
	}
	f.Close()
	if config.Quiet {
		log.SetOutput(ioutil.Discard)
	}

	switch {
	case config.Listen:
		if len(config.Routes) == 0 {
			log.Fatalln("no routes configured! abort...")
		}
		sort.Slice(config.Routes, func(i, j int) bool {
			return config.Routes[i].Addr < config.Routes[j].Addr
		})
		err = distribute(config.Address, config.Proxy, config.Routes)
	default:
		var wg sync.WaitGroup
		for _, r := range config.Routes {
			wg.Add(1)
			go func(r transmit.Route) {
				defer wg.Done()
				if err := forward(config.Address, r, config.Retry); err != nil {
					log.Println(err)
				}
			}(r)
		}
		wg.Wait()
		//err = forward(config.Address, config.Routes)
	}
	if err != nil {
		log.Fatalln(err)
	}
}

func distribute(a, p string, rs []transmit.Route) error {
	r, err := transmit.NewRouter(a, rs)
	if err != nil {
		return err
	}
	log.Printf("start listening on %s", a)
	defer r.Close()

	var wg sync.WaitGroup
	for {
		f, s, err := r.Accept()
		if err != nil {
			log.Printf("connection rejected: %s", err)

			wg.Wait()
			return err
		}
		wg.Add(1)
		go func(r, w net.Conn) {
			defer wg.Done()
			x, err := proxy(p, w.RemoteAddr().String(), rs)
			if err == nil {
				log.Printf("proxy packets from %s to %s", r.RemoteAddr(), x.RemoteAddr())
			}

			log.Printf("start transmitting from %s to %s", r.RemoteAddr(), w.RemoteAddr())
			if err := relay(r, w, x); err != nil && err != ErrDone {
				log.Println("unexpected error while transmitting packets:", err)
			}
			log.Printf("done transmitting from %s to %s", r.RemoteAddr(), w.RemoteAddr())
		}(f, s)
	}
}

func proxy(p, a string, rs []transmit.Route) (net.Conn, error) {
	ix := sort.Search(len(rs), func(i int) bool {
		return rs[i].Addr >= a
	})
	if ix < len(rs) && rs[ix].Addr == a {
		return transmit.Forward(p, rs[ix].Id)
	}
	return nil, fmt.Errorf("no suitable route found for %s", a)
}

func forward(a string, r transmit.Route, c int) error {
	var (
		i    int
		last error
		wait time.Duration
	)
	for {
		i++
		if c > 0 && i > c {
			log.Printf("number of attempts reached (%d)! abort...", c)
			return last
		}
		if i > 1 {
			log.Printf("wait %s before %dth attempt", wait, i)
			<-time.After(wait)
		}
		f, err := transmit.Forward(a, r.Id)
		if err != nil {
			wait = time.Second * time.Duration(i)
			log.Printf("fail to connect to remote host %s: %s", a, err)
			continue
		}
		s, err := transmit.Subscribe(r.Addr, r.Eth)
		if err != nil {
			wait = time.Second * time.Duration(i)
			log.Printf("fail to subscrite to group %s: %s", r.Addr, err)
			continue
		}
		log.Printf("start transmitting from %s to %s", s.LocalAddr(), f.RemoteAddr())
		if err := relay(s, f, nil); err != nil && err != ErrDone {
			log.Println("unexpected error while transmitting packets:", err)
			last = err
		}
		log.Printf("done transmitting from %s to %s", s.LocalAddr(), f.RemoteAddr())
		wait = time.Second
	}
}

type nopCloser struct {
	io.Writer
}

func (n *nopCloser) Close() error { return nil }

func relay(r io.ReadCloser, w, x io.WriteCloser) error {
	defer func() {
		r.Close()
		w.Close()
		if x != nil {
			x.Close()
		}
	}()
	if x != nil {
		w = &nopCloser{io.MultiWriter(x, w)}
	}
	var c uint8
	for {
		_, err := io.Copy(w, r)
		switch err {
		case nil:
		case transmit.ErrCorrupted, transmit.ErrUnknownId:
			log.Println("packet skipped", err)
			c++
			if c == 255 {
				return fmt.Errorf("too many errors! abort")
			}
		case io.EOF, transmit.ErrEmpty:
			return ErrDone
		default:
			if e, ok := err.(net.Error); !ok || !(e.Temporary() || e.Timeout()) {
				return err
			}
		}
	}
}
