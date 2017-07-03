package main

import (
	"encoding/json"
	"errors"
	"flag"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"sync"

	"github.com/midbel/transmit"
)

var ErrDone = errors.New("done")

type Config struct {
	Listen  bool             `json:"-"`
	Verbose bool             `json:"-"`
	Address string           `json:"gateway"`
	Proxy   string           `json:"proxy"`
	Padding int              `json:"padding"`
	Routes  []transmit.Route `json:"routes"`
}

var config *Config

func init() {
	config = new(Config)
	flag.BoolVar(&config.Listen, "l", config.Listen, "listen")
	flag.BoolVar(&config.Verbose, "v", config.Verbose, "verbose")
	flag.Parse()

	f, err := os.Open(flag.Arg(0))
	if err != nil {
		log.Fatalln(err)
	}
	if err := json.NewDecoder(f).Decode(&config); err != nil {
		log.Fatalln(err)
	}
	transmit.Padding = config.Padding
}

func main() {
	var err error
	switch {
	case config.Listen:
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
	defer r.Close()

	var wg sync.WaitGroup
	for {
		f, s, err := r.Accept()
		if err != nil {
			log.Println(err)
			continue
		}
		wg.Add(1)
		go func(r, w net.Conn) {
			var (
				x net.Conn
				i string
			)
			a := w.RemoteAddr()
			for _, r := range rs {
				if r.Addr == a.String() {
					i = r.Id
					break
				}
			}
			if c, err := transmit.Forward(p, i); err == nil {
				x = c
			}
			if err := relay(r, w, x); err != nil && err != ErrDone {
				log.Println(err)
			}
			wg.Done()
		}(f, s)
	}
	wg.Wait()

	return nil
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
			if err := relay(r, w, nil); err != nil && err != ErrDone {
				log.Println(err)
			}
			wg.Done()
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
