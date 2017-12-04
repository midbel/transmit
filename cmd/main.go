package main

import (
	"crypto/rand"
	"io"
	"log"
	random "math/rand"
	"net"
	"os"
	"path/filepath"
	"text/template"
	"time"

	"github.com/midbel/rustine/cli"
)

const BufferSize = 4096 * 4

var commands = []*cli.Command{
	{
		Usage: "relay [-i] [-r] <group> <gateway>",
		Short: "subscribe to multicast group and send packets to a transmit gateway",
		Run:   runRelay,
	},
	{
		Usage: "gateway <gateway> <group>",
		Short: "forward packets received from transmit relay to groups",
		Run:   runGateway,
	},
	{
		Usage: "simulate [-c] [-s] <host:port>",
		Short: "generate random packets",
		Alias: []string{"sim", "alea"},
		Run:   runSimulate,
	},
}

const helpText = `{{.Name}} send packets from one multicast group to another
in differnt networks.

Usage:

  {{.Name}} command [arguments]

The commands are:

{{range .Commands}}{{printf "  %-9s %s" .String .Short}}
{{end}}

Use {{.Name}} [command] -h for more information about its usage.
`

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

func runSimulate(cmd *cli.Command, args []string) error {
	count := cmd.Flag.Int("c", 0, "count")
	size := cmd.Flag.Int("s", 512, "size")
	every := cmd.Flag.Duration("e", time.Second, "every")
	if err := cmd.Flag.Parse(args); err != nil {
		return err
	}

	c, err := net.Dial("udp", cmd.Flag.Arg(0))
	if err != nil {
		return err
	}
	defer c.Close()

	random.Seed(time.Now().Unix())
	if *size < 1 {
		*size = random.Intn(512)
	}
	for i := 0; ; i++ {
		<-time.After(*every)
		if *count > 0 && i >= *count {
			break
		}
		i := random.Intn(*size)
		if _, err := io.CopyN(c, rand.Reader, int64(i)); err != nil {
			return err
		}
	}
	return nil
}
