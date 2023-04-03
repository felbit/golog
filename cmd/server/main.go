package main

import (
	"log"

	"github.com/felbit/golog/internal/server"
)

func main() {
	srv := server.NewHttpServer(":8099")
	log.Fatal(srv.ListenAndServe())
}
