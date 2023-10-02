package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"go.uber.org/zap"

	"github.com/apache/yunikorn-core/pkg/eventdbwriter"
)

const defaultYunikornHost = "yunikorn-service:9080"
const MySQLPort = 3306
const PostgresPort = 5432

func main() {
	// How to obtain these? Configmap/env?
	dbInfo := eventdbwriter.DBInfo{
		Name:     "events",
		User:     "root",
		Password: "rootroot",
		Host:     "localhost",
		Port:     3306,
		Driver:   eventdbwriter.MySQL,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	service := eventdbwriter.CreateEventService(dbInfo, "localhost:9080")
	service.Start(ctx)

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	<-signalChan
	log.Println("Shutdown signal received, exiting...")
}

func getYunikornHost() string {
	yunikornHost := defaultYunikornHost
	if os.Getenv("YUNIKORN_HOSTNAME") != "" {
		yunikornHost = os.Getenv("YUNIKORN_HOSTNAME")
		eventdbwriter.GetLogger().Info("Found YUNIKORN_HOSTNAME variable, overriding default",
			zap.String("hostname", yunikornHost))
	}
	return yunikornHost
}

func getDBInfo() eventdbwriter.DBInfo {
	dbName := os.Getenv("EVENT_DB_NAME")
	if dbName == "" {
		eventdbwriter.GetLogger().Error("Database name undefined, EVENT_DB_NAME is unset")
	}
	user := os.Getenv("EVENT_DB_USER")
	if user == "" {
		eventdbwriter.GetLogger().Error("Database user undefined, EVENT_DB_USER is unset")
	}
	pwd := os.Getenv("EVENT_DB_PASSWORD")
	if pwd == "" {
		eventdbwriter.GetLogger().Error("Database password undefined, EVENT_DB_PASSWORD is unset")
	}
	host := os.Getenv("EVENT_DB_HOST")
	if host == "" {
		eventdbwriter.GetLogger().Error("Database password undefined, EVENT_DB_HOST is unset")
	}

	dbEnv := strings.ToLower(os.Getenv("EVENT_DB_TYPE"))
	var driver eventdbwriter.Driver
	if dbEnv == "" {
		eventdbwriter.GetLogger().Fatal("Database type undefined")
	}
	switch dbEnv {
	case "mysql":
		driver = eventdbwriter.MySQL
	case "postgres":
		driver = eventdbwriter.Postgres
	default:
		eventdbwriter.GetLogger().Fatal("Unsupported database",
			zap.String("db", dbEnv))
	}

	var port int
	portEnv := os.Getenv("EVENT_DB_PORT")
	if portEnv == "" {
		eventdbwriter.GetLogger().Info("Database port unset, using default",
			zap.Int("default", 3306))
		switch driver {
		case eventdbwriter.MySQL:
			port = MySQLPort
		case eventdbwriter.Postgres:
			port = PostgresPort
		default:
			eventdbwriter.GetLogger().Fatal("Unsupported database",
				zap.String("db", dbEnv))
		}
	} else {
		var err error
		port, err = strconv.Atoi(portEnv)
		if err != nil {
			eventdbwriter.GetLogger().Fatal("Illegal value for EVENT_DB_PORT",
				zap.String("value", portEnv))
		}
	}

	return eventdbwriter.DBInfo{
		Name:     dbName,
		Driver:   driver,
		User:     user,
		Host:     host,
		Password: pwd,
		Port:     port,
	}
}
