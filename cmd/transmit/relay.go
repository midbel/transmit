package main

import (
	"os"

	"github.com/midbel/cli"
	"github.com/midbel/toml"
)

var relay = &cli.Command{
	Run:   runRelay,
	Usage: "relay <relay.toml>",
	Short: "",
	Alias: []string{"send"},
	Desc:  ``,
}

type relayer struct {
	Addr  string `toml:"address"`
	Group string `toml:"group"`
	Port  int    `toml:"port"`
}

func runRelay(cmd *cli.Command, args []string) error {
	if err := cmd.Flag.Parse(args); err != nil {
		return err
	}
	f, err := os.Open(cmd.Flag.Arg(0))
	if err != nil {
		return err
	}
	defer f.Close()

	c := struct {
		Addr     string    `toml:"address"`
		Relayers []relayer `toml:"route"`
	}{}
	if err := toml.NewDecoder(f).Decode(&c); err != nil {
		return err
	}
	return nil
}
