package main

import (
	"fmt"
	"log"
	"os"

	"github.com/unhandled-exception/sophiadb/pkg/db"
)

const (
	serverName    = "SophiaDB"
	serverVersion = "0.1.0"
)

func main() {
	logger := newLogger()
	logger.Printf("Server %s starting", version())

	db, err := db.NewDatabase("./sdb_data")
	if err != nil {
		logger.Fatal(err)
	}

	if err := db.Close(); err != nil {
		logger.Fatal(err)
	}

	logger.Print("Server finished")
}

func version() string {
	return fmt.Sprintf("%s/%s", serverName, serverVersion)
}

func newLogger() *log.Logger {
	return log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)
}
