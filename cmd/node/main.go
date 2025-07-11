package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/stathat/consistent"
	"zagreb/pkg/api"
	"zagreb/pkg/nodeapi"
	"zagreb/pkg/router"
	"zagreb/pkg/routerapi"
	"zagreb/pkg/storage/bbolt"
	"zagreb/pkg/types"
)

var (
	nodeID     = flag.String("id", "node-1", "Unique ID for this node")
	nodeAddr   = flag.String("addr", ":8001", "Address this node listens on")
	routerAddr = flag.String("router", "http://localhost:8081", "Address of the router")
)

func registerNode(nodeID, nodeAddr, routerAddr string) (*routerapi.RegisterNodeResponse, error) {
	registration := routerapi.RegisterNodeRequest{
		ID:   nodeID,
		Addr: nodeAddr,
	}
	jsonBytes, err := json.Marshal(registration)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal registration request: %w", err)
	}

	resp, err := http.Post(routerAddr+"/register-node", "application/json", bytes.NewBuffer(jsonBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to register with router: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to register with router, status: %s", resp.Status)
	}

	var registerResp routerapi.RegisterNodeResponse
	if err := json.NewDecoder(resp.Body).Decode(&registerResp); err != nil {
		return nil, fmt.Errorf("failed to decode registration response: %w", err)
	}

	log.Printf("Successfully registered node %s with router", nodeID)
	return &registerResp, nil
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
	registerResp, err := registerNode(*nodeID, *nodeAddr, *routerAddr)
	if err != nil {
		log.Fatalf("failed to register node: %v", err)
	}

	// Initialize consistent hash ring for this node
	aConsistent := consistent.New()
	for _, n := range registerResp.ActiveNodes {
		aConsistent.Add(n.ID)
	}

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

	// Synchronization logic
	routerClient := nodeapi.NewNodeClient(*routerAddr) // Use nodeapi client to talk to router
	listTablesReq := &types.ListTablesRequest{}
	listTablesResp, err := routerClient.ListTables(listTablesReq)
	if err != nil {
		log.Fatalf("failed to list tables from router: %v", err)
	}

	for _, tableName := range listTablesResp.TableNames {
		ownerNodeID, err := aConsistent.Get(tableName)
		if err != nil {
			log.Printf("could not determine owner for table %s: %v", tableName, err)
			continue
		}

		if ownerNodeID == *nodeID {
			// This node is responsible for the table, try to sync data
			log.Printf("Node %s is responsible for table %s. Attempting to sync.", *nodeID, tableName)

			// Find another active node that is also responsible for this table
			var sourceNode router.Node
			for _, n := range registerResp.ActiveNodes {
				if n.ID != *nodeID {
					sourceNodeID, err := aConsistent.Get(tableName)
					if err == nil && sourceNodeID == n.ID {
						sourceNode = n
						break
					}
				}
			}

			if sourceNode.ID != "" {
				log.Printf("Syncing table %s from node %s (%s)", tableName, sourceNode.ID, sourceNode.Addr)
				sourceClient := nodeapi.NewNodeClient(sourceNode.Addr)
				
				var allSyncedItems []map[string]*types.AttributeValue
				scanReq := &types.ScanRequest{TableName: tableName}
				
				for {
					resp, err := sourceClient.InternalScan(scanReq)
					if err != nil {
						log.Printf("failed to internal scan table %s from %s: %v", tableName, sourceNode.ID, err)
						break // Exit pagination loop on error
					}

					allSyncedItems = append(allSyncedItems, resp.Items...)

					if resp.LastEvaluatedKey == nil {
						break // No more pages
					}
					scanReq.ExclusiveStartKey = resp.LastEvaluatedKey
				}

				for _, item := range allSyncedItems {
					putReq := &types.PutRequest{TableName: tableName, Item: item}
					if err := bboltStorage.Put(putReq); err != nil {
						log.Printf("failed to put item into local storage for table %s: %v", tableName, err)
					}
				}
				log.Printf("Finished syncing %d items for table %s.", len(allSyncedItems), tableName)
			} else {
				log.Printf("No other active node found to sync table %s from. Starting with empty data.", tableName)
			}
		}
	}

	log.Printf("Node %s synchronization complete. Starting server.", *nodeID)
	server := api.NewServer(bboltStorage)
	server.Run(*nodeAddr)
}
