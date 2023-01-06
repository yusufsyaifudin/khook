package main

import (
	"fmt"
	"github.com/urfave/cli/v3"
	"github.com/yusufsyaifudin/khook/cmd/server"
	"log"
	"os"
)

func main() {
	app := &cli.App{}
	app.DefaultCommand = "server"
	app.Commands = []*cli.Command{
		{
			Name:        "server",
			Description: "Run khook server",
			Category:    "server",
			Before:      nil,
			After:       nil,
			Action: func(context *cli.Context) error {
				srv := &server.Server{}
				return srv.Run()
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		err = fmt.Errorf("cannot run app: %w", err)
		log.Fatalln(err)
	}
}
