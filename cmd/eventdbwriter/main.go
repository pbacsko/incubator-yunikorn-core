package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/apache/yunikorn-core/pkg/eventdbwriter"
)

func main() {
	// How to obtain these? Configmap/env?
	dbInfo := eventdbwriter.DBInfo{
		Name:     "events",
		User:     "root",
		Password: "rootroot",
		Host:     "localhost",
		Port:     3306,
	}

	eventdbwriter.StartEventService(dbInfo, "localhost:9080")

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	for range signalChan {
		log.Println("Shutdown signal received, exiting...")
		os.Exit(0)
	}
}
