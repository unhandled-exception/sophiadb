package main

import (
	"fmt"
	"log"
	"os"
)

const (
	serverName    = "SophiaDB"
	serverVersion = "0.1.0"
)

func main() {
	logger := newLogger()
	logger.Printf("Server %s starting", version())

	logger.Print("Server finished")
}

func version() string {
	return fmt.Sprintf("%s/%s", serverName, serverVersion)
}

func newLogger() *log.Logger {
	return log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)
}
