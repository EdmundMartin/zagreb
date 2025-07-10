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
}

// NodeClientFactory creates a new node client.
type NodeClientFactory interface {
	NewNodeClient(addr string) storage.Storage
}

type defaultNodeClientFactory struct{}

func (f *defaultNodeClientFactory) NewNodeClient(addr string) storage.Storage {
	return nodeapi.NewNodeClient(addr)
}

// Router implements the Storage interface and routes requests to appropriate nodes.
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

	r.consistent.Add(node.ID)
	r.nodes[node.ID] = node
	client := r.nodeClientFactory.NewNodeClient(node.Addr)
	r.nodeClients[node.ID] = client
}

// RemoveNode removes a node from the consistent hash ring.
func (r *Router) RemoveNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

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
func (r *Router) CreateTable(req *types.CreateTableRequest) (*types.CreateTableResponse, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.nodes) == 0 {
		return nil, fmt.Errorf("no nodes in the ring to create table")
	}

	var firstResp *types.CreateTableResponse
	var firstErr error

	for _, node := range r.nodes {
		client, err := r.getClientForNode(node)
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("failed to get client for node %s: %w", node.ID, err)
			}
			continue
		}
		resp, err := client.CreateTable(req)
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("failed to create table on node %s: %w", node.ID, err)
			}
		} else if firstResp == nil {
			firstResp = resp
		}
	}

	if firstErr != nil {
		return nil, firstErr
	}
	if firstResp == nil {
		return nil, fmt.Errorf("no successful responses from nodes for CreateTable")
	}
	return firstResp, nil
}

// DeleteTable routes the DeleteTable request to the appropriate node.
func (r *Router) DeleteTable(req *types.DeleteTableRequest) (*types.DeleteTableResponse, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.nodes) == 0 {
		return nil, fmt.Errorf("no nodes in the ring to delete table")
	}

	var firstResp *types.DeleteTableResponse
	var firstErr error

	for _, node := range r.nodes {
		client, err := r.getClientForNode(node)
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("failed to get client for node %s: %w", node.ID, err)
			}
			continue
		}
		resp, err := client.DeleteTable(req)
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("failed to delete table on node %s: %w", node.ID, err)
			}
		} else if firstResp == nil {
			firstResp = resp
		}
	}

	if firstErr != nil {
		return nil, firstErr
	}
	if firstResp == nil {
		return nil, fmt.Errorf("no successful responses from nodes for DeleteTable")
	}
	return firstResp, nil
}

// DescribeTable routes the DescribeTable request to the appropriate node.
func (r *Router) DescribeTable(req *types.DescribeTableRequest) (*types.DescribeTableResponse, error) {
	node, err := r.GetNode(req.TableName)
	if err != nil {
		return nil, err
	}
	client, err := r.getClientForNode(node)
	if err != nil {
		return nil, err
	}
	return client.DescribeTable(req)
}

// ListTables routes the ListTables request to all nodes and aggregates the results.
func (r *Router) ListTables(req *types.ListTablesRequest) (*types.ListTablesResponse, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.nodes) == 0 {
		return nil, fmt.Errorf("no nodes in the ring")
	}

	allTableNames := make(map[string]struct{})
	for _, node := range r.nodes {
		client, err := r.getClientForNode(node)
		if err != nil {
			return nil, err
		}
		resp, err := client.ListTables(req)
		if err != nil {
			return nil, err
		}
		for _, tableName := range resp.TableNames {
			allTableNames[tableName] = struct{}{}
		}
	}

	result := make([]string, 0, len(allTableNames))
	for tableName := range allTableNames {
		result = append(result, tableName)
	}

	return &types.ListTablesResponse{TableNames: result}, nil
}

// Put routes the Put request to the appropriate node.
func (r *Router) Put(req *types.PutRequest) error {
	node, err := r.GetNode(req.TableName)
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
	node, err := r.GetNode(req.TableName)
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
	node, err := r.GetNode(req.TableName)
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
	node, err := r.GetNode(req.TableName)
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
	node, err := r.GetNode(req.TableName)
	if err != nil {
		return nil, err
	}
	client, err := r.getClientForNode(node)
	if err != nil {
		return nil, err
	}
	return client.Query(req)
}
