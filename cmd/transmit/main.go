package main

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"text/template"

	"github.com/midbel/cli"
	"github.com/midbel/transmit"
)

const helpText = `{{.Name}} contains various actions to monitor system activities.

Usage:

  {{.Name}} command [arguments]

The commands are:

{{range .Commands}}{{printf "  %-9s %s" .String .Short}}
{{end}}

Use {{.Name}} [command] -h for more information about its usage.
`

var commands = []*cli.Command{
	{
		Run:   runDumper,
		Usage: "dump <host:port>",
		Short: "dump packets received like hexdump -C -v",
	},
	{
		Run:   runSimulate,
		Usage: "simulate [-q] [-r] [-e] [-c] [-s] [-p] [-z] [-t] <host:port...>",
		Short: "generate random packets and send them to the specify addresses",
		Alias: []string{"generate", "sim", "gen"},
		Desc: `

options:
	-p proto  use the specify protocol (tcp or udp)
	-c count  write count packets to group then exit
	-e every  write a packet every given elapsed interval to group
	-s size   write packet of size bytes to group
	-t rate   limit outgoing bandwidth
	-z        write packet with only zeros
	-r        write packet of random size to group with upper limit set to size
	-q        suppress debug information from stderr
`,
	},
	{
		Run:   runRelay,
		Usage: "relay <relay.toml>",
		Short: "",
		Alias: []string{"send"},
		Desc:  ``,
	},
	{
		Run:   runGateway,
		Usage: "gateway <config.toml>",
		Short: "",
		Alias: []string{"recv", "listen", "gw"},
		Desc:  ``,
	},
	{
		Run:   runSplit2,
		Usage: "split2 <config.toml>",
		Short: "split and send fragmented packets",
	},
	{
		Run:   runMerge2,
		Usage: "merge2 <config.toml>",
		Short: "merge and send fragmented packets",
	},
	{
		Run:   runSplit,
		Usage: "split [-b] [-n] [-k] [-r] [-s] [-y] <remote> <local,...>",
		Alias: []string{"disassemble"},
		Short: "split and send fragmented packets",
		Desc: `
options:
  -b block  split incoming packets by chunks of block bytes (default: 1K)
  -n count  use count outgoing connection(s) (default: 4)
  -k keep   use same rate for all outgoing connections(s) (default: false)
  -r rate   specify bandwidth by requested connections (default: 8m)
  -s size   buffer to read from incoming connections (default: 32K)
  -y        use system clock (default: false)

notes on UNIT:
b, k, kb, m, mb, g, gb   bits, kilobits, megabits and gigabits (the b is not required for k, m and g)
B, K, KB, M, MB, G, GB   bytes, kilobytes, megabytes and gigabytes (the B is not required for K, M and G)

The default unit assumed when no unit is given is the bytes.
`,
	},
	{
		Run:   runMerge,
		Usage: "merge <local> <remote,...>",
		Alias: []string{"reassemble"},
		Short: "merge and send fragmented packets",
		Desc:  ``,
	},
}

func init() {
	transmit.Logger.SetOutput(os.Stderr)
}

func main() {
	log.SetFlags(0)
	usage := func() {
		data := struct {
			Name     string
			Commands []*cli.Command
		}{
			Name:     filepath.Base(os.Args[0]),
			Commands: commands,
		}
		t := template.Must(template.New("help").Parse(helpText))
		t.Execute(os.Stderr, data)

		os.Exit(2)
	}
	if err := cli.Run(commands, usage, nil); err != nil {
		log.Fatalln(err)
	}
}

type cert struct {
	Policy   string `toml:"policy"`
	Name     string `toml:"server"`
	Root     string `toml:"root"`
	CertFile string `toml:"cert"`
	KeyFile  string `toml:"key"`
	Insecure bool   `toml:"insecure"`

	config *tls.Config
}

func (c cert) Server() *tls.Config {
	cert := c.Client()
	if cert == nil {
		return cert
	}
	cert.ClientCAs = cert.RootCAs
	switch c.Policy {
	case "request":
		cert.ClientAuth = tls.RequestClientCert
	case "require":
		cert.ClientAuth = tls.RequireAnyClientCert
	case "verify":
		cert.ClientAuth = tls.VerifyClientCertIfGiven
	case "none":
		cert.ClientAuth = tls.NoClientCert
	default:
		cert.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return cert
}

func (c cert) Client() *tls.Config {
	if c.config != nil {
		return c.config
	}
	cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
	if err != nil {
		return nil
	}
	c.config = &tls.Config{
		ServerName:         c.Name,
		Certificates:       []tls.Certificate{cert},
		InsecureSkipVerify: c.Insecure,
	}
	if is, err := ioutil.ReadDir(c.Root); err == nil {
		p := x509.NewCertPool()
		for _, i := range is {
			bs, err := ioutil.ReadFile(filepath.Join(c.Root, i.Name()))
			if err != nil {
				continue
			}
			if ok := p.AppendCertsFromPEM(bs); !ok {
				log.Printf("fail to add certificate to %s", i.Name())
			}
		}
		c.config.RootCAs = p
	}
	return c.config
}
