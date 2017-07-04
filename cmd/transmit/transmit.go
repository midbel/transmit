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

	"github.com/midbel/transmit"
)

var ErrDone = errors.New("done")

type Config struct {
	Listen  bool             `json:"-"`
	Verbose bool             `json:"-"`
	Address string           `json:"gateway"`
	Proxy   string           `json:"proxy"`
	Routes  []transmit.Route `json:"routes"`
}

func main() {
	config := new(Config)
	flag.BoolVar(&config.Listen, "l", config.Listen, "listen")
	flag.BoolVar(&config.Verbose, "v", config.Verbose, "verbose")
	flag.Parse()

	f, err := os.Open(flag.Arg(0))
	if err != nil {
		log.Fatalln(err)
	}
	defer f.Close()
	if err := json.NewDecoder(f).Decode(&config); err != nil {
		log.Fatalln(err)
	}

	switch {
	case config.Listen:
		sort.Slice(config.Routes, func(i, j int) bool {
			return config.Routes[i].Addr < config.Routes[j].Addr
		})
		err = distribute(config.Address, config.Proxy, config.Routes)
	default:
		err = forward(config.Address, config.Routes)
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
			continue
		}
		wg.Add(1)
		go func(r, w net.Conn) {
			x, err := proxy(p, w.RemoteAddr().String(), rs)
			if err == nil {
				log.Printf("proxy packets from %s to %s", r.RemoteAddr(), x.RemoteAddr())
			} else {
				log.Println(err)
			}

			log.Printf("start transmitting from %s to %s", r.RemoteAddr(), w.RemoteAddr())
			if err := relay(r, w, x); err != nil && err != ErrDone {
				log.Println("unexpected error while transmitting packets:", err)
			}
			wg.Done()
			log.Printf("done transmitting from %s to %s", r.RemoteAddr(), w.RemoteAddr())
		}(f, s)
	}
	wg.Wait()

	return nil
}

func proxy(p, a string, rs []transmit.Route) (net.Conn, error) {
	ix := sort.Search(len(rs), func(i int) bool {
		return rs[i].Addr >= a
	})
	if ix < len(rs) && rs[ix].Addr == a {
		return transmit.Proxy(p, rs[ix].Id)
	}
	return nil, fmt.Errorf("no suitable route found for %s", a)
}

func forward(a string, rs []transmit.Route) error {
	var wg sync.WaitGroup
	for _, r := range rs {
		f, err := transmit.Forward(a, r.Id)
		if err != nil {
			return err
		}
		s, err := transmit.Subscribe(r.Addr, r.Eth)
		if err != nil {
			return err
		}
		wg.Add(1)
		go func(r, w net.Conn) {
			log.Printf("start transmitting from %s to %s", r.LocalAddr(), w.RemoteAddr())
			if err := relay(r, w, nil); err != nil && err != ErrDone {
				log.Println("unexpected error while transmitting packets:", err)
			}
			wg.Done()
			log.Printf("done transmitting from %s to %s", r.LocalAddr(), w.RemoteAddr())
		}(s, f)
	}
	wg.Wait()
	return nil
}

func relay(r io.ReadCloser, w, x io.WriteCloser) error {
	defer func() {
		r.Close()
		w.Close()
		if x != nil {
			x.Close()
		}
	}()
	if x != nil {
		r = ioutil.NopCloser(io.TeeReader(r, x))
	}
	for {
		_, err := io.Copy(w, r)
		switch err {
		case nil:
		case transmit.ErrCorrupted:
			log.Println(err)
		case io.EOF:
			return ErrDone
		default:
			if e, ok := err.(net.Error); !ok || !(e.Temporary() || e.Timeout()) {
				return err
			}
		}
	}
	return nil
}
