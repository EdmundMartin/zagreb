package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"zagreb/pkg/api"
	"zagreb/pkg/routerapi"
	"zagreb/pkg/storage/bbolt"
)

var (
	nodeID     = flag.String("id", "node-1", "Unique ID for this node")
	nodeAddr   = flag.String("addr", ":8001", "Address this node listens on")
	routerAddr = flag.String("router", "http://localhost:8081", "Address of the router")
)

func registerNode(nodeID, nodeAddr, routerAddr string) {
	registration := routerapi.RegisterNodeRequest{
		ID:   nodeID,
		Addr: nodeAddr,
	}
	jsonBytes, err := json.Marshal(registration)
	if err != nil {
		log.Fatalf("failed to marshal registration request: %v", err)
	}

	resp, err := http.Post(routerAddr+"/register-node", "application/json", bytes.NewBuffer(jsonBytes))
	if err != nil {
		log.Fatalf("failed to register with router: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Fatalf("failed to register with router, status: %s", resp.Status)
	}
	log.Printf("Successfully registered node %s with router", nodeID)
}

func deregisterNode(nodeID, routerAddr string) {
	deregistration := routerapi.DeregisterNodeRequest{
		ID: nodeID,
	}
	jsonBytes, err := json.Marshal(deregistration)
	if err != nil {
		log.Printf("failed to marshal deregistration request: %v", err)
		return
	}

	req, err := http.NewRequest("POST", routerAddr+"/deregister-node", bytes.NewBuffer(jsonBytes))
	if err != nil {
		log.Printf("failed to create deregistration request: %v", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("failed to deregister with router: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("failed to deregister with router, status: %s", resp.Status)
	}
	log.Printf("Successfully deregistered node %s from router", nodeID)
}

func main() {
	flag.Parse()

	// Register node with router on startup
	registerNode(*nodeID, *nodeAddr, *routerAddr)

	// Handle graceful shutdown
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		deregisterNode(*nodeID, *routerAddr)
		os.Exit(0)
	}()

	dbPath := "./" + *nodeID + ".db"
	bboltStorage, err := bbolt.NewBBoltStorage(dbPath)
	if err != nil {
		log.Fatalf("failed to create bbolt storage: %v", err)
	}

	server := api.NewServer(bboltStorage)
	server.Run(*nodeAddr)
}
