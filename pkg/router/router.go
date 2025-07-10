package router

import (
	"fmt"
	"sync"

	"github.com/stathat/consistent"
	"zagreb/pkg/expression"
	"zagreb/pkg/nodeapi"
	"zagreb/pkg/storage"
	"zagreb/pkg/types"
)

// Node represents a storage node in the distributed system.
type Node struct {
	ID   string
	Addr string
	// Add any other node-specific information here, e.g., client for communication
}

// Router implements the Storage interface and routes requests to appropriate nodes.
type NodeClientFactory interface {
	NewNodeClient(addr string) storage.Storage
}

type defaultNodeClientFactory struct{}

func (f *defaultNodeClientFactory) NewNodeClient(addr string) storage.Storage {
	return nodeapi.NewNodeClient(addr)
}

type Router struct {
	consistent        *consistent.Consistent
	nodes             map[string]Node // Map node ID to Node struct
	mu                sync.RWMutex
	nodeClients       map[string]storage.Storage // Map node ID to its storage client
	nodeClientFactory NodeClientFactory
}

// NewRouter creates a new Router instance.
func NewRouter(factory NodeClientFactory) *Router {
	if factory == nil {
		factory = &defaultNodeClientFactory{}
	}
	return &Router{
		consistent:        consistent.New(),
		nodes:             make(map[string]Node),
		nodeClients:       make(map[string]storage.Storage),
		nodeClientFactory: factory,
	}
}

// AddNode adds a new node to the consistent hash ring.
func (r *Router) AddNode(node Node) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Add to consistent hash ring
	r.consistent.Add(node.ID)
	r.nodes[node.ID] = node

	// Create and set client for the node
	client := r.nodeClientFactory.NewNodeClient(node.Addr)
	r.nodeClients[node.ID] = client
}

// RemoveNode removes a node from the consistent hash ring.
func (r *Router) RemoveNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Remove from consistent hash ring
	r.consistent.Remove(nodeID)
	delete(r.nodes, nodeID)
	delete(r.nodeClients, nodeID)
}

// GetNode returns the node responsible for the given key.
func (r *Router) GetNode(key string) (Node, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.consistent.Members()) == 0 {
		return Node{}, fmt.Errorf("no nodes in the ring")
	}

	nodeID, err := r.consistent.Get(key)
	if err != nil {
		return Node{}, fmt.Errorf("failed to get node from consistent hash ring: %w", err)
	}

	node, ok := r.nodes[nodeID]
	if !ok {
		return Node{}, fmt.Errorf("node %s found in ring but not in node map", nodeID)
	}

	return node, nil
}

// getClientForNode retrieves the storage client for a given node.
func (r *Router) getClientForNode(node Node) (storage.Storage, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	client, ok := r.nodeClients[node.ID]
	if !ok {
		return nil, fmt.Errorf("no client found for node %s", node.ID)
	}
	return client, nil
}

// CreateTable routes the CreateTable request to the appropriate node.
func (r *Router) CreateTable(req *types.CreateTableRequest) error {
	node, err := r.GetNode(req.TableName)
	if err != nil {
		return err
	}
	client, err := r.getClientForNode(node)
	if err != nil {
		return err
	}
	return client.CreateTable(req)
}

// Put routes the Put request to the appropriate node.
func (r *Router) Put(req *types.PutRequest) error {
	node, err := r.GetNode(req.TableName + "#" + *req.Item[req.TableName].M[req.TableName].S) // Assuming HashKey is TableName.HashKey
	if err != nil {
		return err
	}
	client, err := r.getClientForNode(node)
	if err != nil {
		return err
	}
	return client.Put(req)
}

// Get routes the Get request to the appropriate node.
func (r *Router) Get(req *types.GetRequest) (map[string]*expression.AttributeValue, error) {
	node, err := r.GetNode(req.TableName + "#" + *req.Key[req.TableName].S) // Assuming HashKey is TableName.HashKey
	if err != nil {
		return nil, err
	}
	client, err := r.getClientForNode(node)
	if err != nil {
		return nil, err
	}
	return client.Get(req)
}

// Delete routes the Delete request to the appropriate node.
func (r *Router) Delete(req *types.DeleteRequest) error {
	node, err := r.GetNode(req.TableName + "#" + *req.Key[req.TableName].S) // Assuming HashKey is TableName.HashKey
	if err != nil {
		return err
	}
	client, err := r.getClientForNode(node)
	if err != nil {
		return err
	}
	return client.Delete(req)
}

// Update routes the Update request to the appropriate node.
func (r *Router) Update(req *types.UpdateRequest) (map[string]*expression.AttributeValue, error) {
	node, err := r.GetNode(req.TableName + "#" + *req.Key[req.TableName].S) // Assuming HashKey is TableName.HashKey
	if err != nil {
		return nil, err
	}
	client, err := r.getClientForNode(node)
	if err != nil {
		return nil, err
	}
	return client.Update(req)
}

// Query routes the Query request to the appropriate node.
func (r *Router) Query(req *types.QueryRequest) ([]map[string]*expression.AttributeValue, error) {
	node, err := r.GetNode(req.TableName + "#" + req.KeyConditionExpression) // Assuming HashKey is TableName.HashKey
	if err != nil {
		return nil, err
	}
	client, err := r.getClientForNode(node)
	if err != nil {
		return nil, err
	}
	return client.Query(req)
}
