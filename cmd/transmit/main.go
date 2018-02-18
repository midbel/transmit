package main

import (
	"encoding/binary"
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"text/template"

	"github.com/midbel/cli"
)

const helpText = `{{.Name}} contains various actions to monitor system activities.

Usage:

  {{.Name}} command [arguments]

The commands are:

{{range .Commands}}{{printf "  %-9s %s" .String .Short}}
{{end}}

Use {{.Name}} [command] -h for more information about its usage.
`

var ErrDone = errors.New("done")

type Packet struct {
	Port     uint16
	Sequence uint32
	Length   uint32
	Payload  []byte
}

func main() {
	commands := []*cli.Command{
		relay,
		gateway,
		simulate,
	}
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

func DecodePacket(r io.Reader) (*Packet, error) {
	p := new(Packet)
	binary.Read(r, binary.BigEndian, &p.Port)
	binary.Read(r, binary.BigEndian, &p.Sequence)
	binary.Read(r, binary.BigEndian, &p.Length)

	p.Payload = make([]byte, int(p.Length))
	if _, err := io.ReadFull(r, p.Payload); err != nil {
		return nil, err
	}
	return p, nil
}
