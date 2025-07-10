package main

import (
	"zagreb/pkg/api"
	"zagreb/pkg/router"
)

func main() {
	// Create a new router
	r := router.NewRouter(nil)

	server := api.NewRouterServer(r)
	server.Run(":8081") // Router listens on port 8000
}