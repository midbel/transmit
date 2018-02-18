package main

import (
	"errors"
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
