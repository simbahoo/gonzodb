package main

import (
	"flag"

	"github.com/gonzodb/gonzo"
	"github.com/ngaut/log"
)

var storePath = flag.String("path", "127.0.0.1:22379/pd?cluster=1", "gonzo storage path")

func main() {
	flag.Parse()
	log.SetLevelByString("info")

	server, err := gonzo.NewServerAddr("tcp", ":28018", *storePath)
	if err != nil {
		log.Fatal(err)
	}
	server.Start()
	err = server.Wait()
	if err != nil {
		log.Fatal(err)
	}
}
