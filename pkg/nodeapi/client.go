package nodeapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"zagreb/pkg/expression"
	"zagreb/pkg/storage"
	"zagreb/pkg/types"
)

// NodeClient implements the storage.Storage interface for communicating with a node.
type NodeClient struct {
	Addr string
	client *http.Client
}

// NewNodeClient creates a new NodeClient.
func NewNodeClient(addr string) storage.Storage {
	return &NodeClient{
		Addr: addr,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (c *NodeClient) doRequest(action string, reqBody interface{}, respBody interface{}) error {
	requestPayload := map[string]interface{}{
		"Action": action,
	}

	// Marshal the specific request body into the generic payload
	// This is a bit hacky, but necessary to embed the specific request type
	// into the generic map expected by the server.
	jsonBytes, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("failed to marshal request body: %w", err)
	}

	var specificReqMap map[string]interface{}
	if err := json.Unmarshal(jsonBytes, &specificReqMap); err != nil {
		return fmt.Errorf("failed to unmarshal specific request to map: %w", err)
	}

	for k, v := range specificReqMap {
		requestPayload[k] = v
	}

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(requestPayload); err != nil {
		return fmt.Errorf("failed to encode request payload: %w", err)
	}

	url := fmt.Sprintf("http://%s/", c.Addr) // Always POST to root
	httpReq, err := http.NewRequest("POST", url, &buf)
	if err != nil {
		return fmt.Errorf("failed to create HTTP request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	httpResp, err := c.client.Do(httpReq)
	if err != nil {
		return fmt.Errorf("failed to send HTTP request: %w", err)
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode != http.StatusOK {
		return fmt.Errorf("node responded with status: %s", httpResp.Status)
	}

	if respBody != nil {
		if err := json.NewDecoder(httpResp.Body).Decode(respBody); err != nil {
			return fmt.Errorf("failed to decode response body: %w", err)
		}
	}

	return nil
}

// CreateTable sends a CreateTable request to the node.
func (c *NodeClient) CreateTable(req *types.CreateTableRequest) (*types.CreateTableResponse, error) {
	var resp types.CreateTableResponse
	err := c.doRequest("CreateTable", req, &resp)
	return &resp, err
}

// DeleteTable sends a DeleteTable request to the node.
func (c *NodeClient) DeleteTable(req *types.DeleteTableRequest) (*types.DeleteTableResponse, error) {
	var resp types.DeleteTableResponse
	err := c.doRequest("DeleteTable", req, &resp)
	return &resp, err
}

// DescribeTable sends a DescribeTable request to the node.
func (c *NodeClient) DescribeTable(req *types.DescribeTableRequest) (*types.DescribeTableResponse, error) {
	var resp types.DescribeTableResponse
	err := c.doRequest("DescribeTable", req, &resp)
	return &resp, err
}

// ListTables sends a ListTables request to the node.
func (c *NodeClient) ListTables(req *types.ListTablesRequest) (*types.ListTablesResponse, error) {
	var resp types.ListTablesResponse
	err := c.doRequest("ListTables", req, &resp)
	return &resp, err
}

// Put sends a Put request to the node.
func (c *NodeClient) Put(req *types.PutRequest) error {
	return c.doRequest("PutItem", req, nil)
}

// Get sends a Get request to the node and returns the item.
func (c *NodeClient) Get(req *types.GetRequest) (map[string]*expression.AttributeValue, error) {
	var item map[string]*expression.AttributeValue
	err := c.doRequest("GetItem", req, &item)
	return item, err
}

// Delete sends a Delete request to the node.
func (c *NodeClient) Delete(req *types.DeleteRequest) error {
	return c.doRequest("DeleteItem", req, nil)
}

// Update sends an Update request to the node and returns the updated item.
func (c *NodeClient) Update(req *types.UpdateRequest) (map[string]*expression.AttributeValue, error) {
	var item map[string]*expression.AttributeValue
	err := c.doRequest("UpdateItem", req, &item)
	return item, err
}

// Query sends a Query request to the node and returns the items.
func (c *NodeClient) Query(req *types.QueryRequest) ([]map[string]*expression.AttributeValue, error) {
	var items []map[string]*expression.AttributeValue
	err := c.doRequest("Query", req, &items)
	return items, err
}

// Scan sends a Scan request to the node and returns the items.
func (c *NodeClient) Scan(req *types.ScanRequest) (*types.ScanResponse, error) {
	var resp types.ScanResponse
	err := c.doRequest("Scan", req, &resp)
	return &resp, err
}

// InternalScan sends an internal Scan request to the node and returns the items.
func (c *NodeClient) InternalScan(req *types.ScanRequest) (*types.ScanResponse, error) {
	var resp types.ScanResponse
	err := c.doRequest("InternalScan", req, &resp)
	return &resp, err
}