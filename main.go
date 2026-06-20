package main

import (
	"order-service/internal/app"
	"os"

	log "github.com/sirupsen/logrus"
)

func main() {
	log.SetOutput(os.Stdout)
	app.Run()
}
