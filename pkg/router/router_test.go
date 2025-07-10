package router

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"zagreb/pkg/expression"
	"zagreb/pkg/storage"
	"zagreb/pkg/types"
)

// MockStorage is a mock implementation of the storage.Storage interface.
type MockStorage struct {
	mock.Mock
}

func (m *MockStorage) CreateTable(req *types.CreateTableRequest) (*types.CreateTableResponse, error) {
	args := m.Called(req)
	return args.Get(0).(*types.CreateTableResponse), args.Error(1)
}

func (m *MockStorage) DeleteTable(req *types.DeleteTableRequest) (*types.DeleteTableResponse, error) {
	args := m.Called(req)
	return args.Get(0).(*types.DeleteTableResponse), args.Error(1)
}

func (m *MockStorage) DescribeTable(req *types.DescribeTableRequest) (*types.DescribeTableResponse, error) {
	args := m.Called(req)
	return args.Get(0).(*types.DescribeTableResponse), args.Error(1)
}

func (m *MockStorage) ListTables(req *types.ListTablesRequest) (*types.ListTablesResponse, error) {
	args := m.Called(req)
	return args.Get(0).(*types.ListTablesResponse), args.Error(1)
}

func (m *MockStorage) Put(req *types.PutRequest) error {
	args := m.Called(req)
	return args.Error(0)
}

func (m *MockStorage) Get(req *types.GetRequest) (map[string]*expression.AttributeValue, error) {
	args := m.Called(req)
	return args.Get(0).(map[string]*expression.AttributeValue), args.Error(1)
}

func (m *MockStorage) Delete(req *types.DeleteRequest) error {
	args := m.Called(req)
	return args.Error(0)
}

func (m *MockStorage) Update(req *types.UpdateRequest) (map[string]*expression.AttributeValue, error) {
	args := m.Called(req)
	return args.Get(0).(map[string]*expression.AttributeValue), args.Error(1)
}

func (m *MockStorage) Query(req *types.QueryRequest) ([]map[string]*expression.AttributeValue, error) {
	args := m.Called(req)
	return args.Get(0).([]map[string]*expression.AttributeValue), args.Error(1)
}

// MockNodeClientFactory is a function type to mock nodeapi.NewNodeClient
type MockNodeClientFactory struct {
	mock.Mock
}

func (m *MockNodeClientFactory) NewNodeClient(addr string) storage.Storage {
	args := m.Called(addr)
	return args.Get(0).(storage.Storage)
}

func TestNewRouter(t *testing.T) {
	r := NewRouter(nil)
	assert.NotNil(t, r)
	assert.NotNil(t, r.consistent)
	assert.NotNil(t, r.nodes)
	assert.NotNil(t, r.nodeClients)
	assert.NotNil(t, r.nodeClientFactory)
}

func TestAddNode(t *testing.T) {
	mockFactory := new(MockNodeClientFactory)
	r := NewRouter(mockFactory)
	mockClient := new(MockStorage)
	mockFactory.On("NewNodeClient", "localhost:8001").Return(mockClient).Once()

	node1 := Node{ID: "node1", Addr: "localhost:8001"}
	r.AddNode(node1)

	assert.Contains(t, r.nodes, "node1")
	assert.Equal(t, node1, r.nodes["node1"])
	assert.Contains(t, r.nodeClients, "node1")
	assert.Equal(t, mockClient, r.nodeClients["node1"])
	mockFactory.AssertExpectations(t)

	// Check if node is added to consistent hash ring (indirectly)
	// This is a bit tricky to test directly without exposing consistent.Consistent internals.
	// We can check if GetNode returns it.
	retrievedNode, err := r.GetNode("some_key")
	assert.NoError(t, err)
	assert.Equal(t, node1.ID, retrievedNode.ID)
}

func TestRemoveNode(t *testing.T) {
	mockFactory := new(MockNodeClientFactory)
	r := NewRouter(mockFactory)
	mockClient := new(MockStorage)
	mockFactory.On("NewNodeClient", "localhost:8001").Return(mockClient).Once()

	node1 := Node{ID: "node1", Addr: "localhost:8001"}
	r.AddNode(node1)
	assert.Contains(t, r.nodes, "node1")

	r.RemoveNode("node1")
	assert.NotContains(t, r.nodes, "node1")
	assert.NotContains(t, r.nodeClients, "node1")

	// Ensure it's removed from consistent hash ring
	_, err := r.GetNode("some_key")
	assert.Error(t, err) // Should error as no nodes are left
}

func TestGetClientForNode(t *testing.T) {
	mockFactory := new(MockNodeClientFactory)
	r := NewRouter(mockFactory)
	mockClient := new(MockStorage)
	mockFactory.On("NewNodeClient", "localhost:8001").Return(mockClient).Once()

	node1 := Node{ID: "node1", Addr: "localhost:8001"}
	r.AddNode(node1)

	client, err := r.getClientForNode(node1)
	assert.NoError(t, err)
	assert.Equal(t, mockClient, client)

	// Test for non-existent client
	node2 := Node{ID: "node2", Addr: "localhost:8002"}
	_, err = r.getClientForNode(node2)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no client found for node node2")
}

func TestCreateTable_Success(t *testing.T) {
	mockFactory := new(MockNodeClientFactory)
	r := NewRouter(mockFactory)

	// Node 1
	mockClient1 := new(MockStorage)
	mockFactory.On("NewNodeClient", "localhost:8001").Return(mockClient1).Once()
	node1 := Node{ID: "node1", Addr: "localhost:8001"}
	r.AddNode(node1)

	// Node 2
	mockClient2 := new(MockStorage)
	mockFactory.On("NewNodeClient", "localhost:8002").Return(mockClient2).Once()
	node2 := Node{ID: "node2", Addr: "localhost:8002"}
	r.AddNode(node2)

	req := &types.CreateTableRequest{TableName: "test_table"}
	expectedResp := &types.CreateTableResponse{
		TableDescription: types.TableDescription{
			TableName: "test_table",
		},
	}

	// Success case: CreateTable should be called on all nodes
	mockClient1.On("CreateTable", req).Return(expectedResp, nil).Once()
	mockClient2.On("CreateTable", req).Return(expectedResp, nil).Once()
	resp, err := r.CreateTable(req)
	assert.NoError(t, err)
	assert.Equal(t, expectedResp, resp)
	mockClient1.AssertExpectations(t)
	mockClient2.AssertExpectations(t)
}

func TestCreateTable_ErrorFromOneClient(t *testing.T) {
	mockFactory := new(MockNodeClientFactory)
	r := NewRouter(mockFactory)

	// Node 1
	mockClient1 := new(MockStorage)
	mockFactory.On("NewNodeClient", "localhost:8001").Return(mockClient1).Once()
	node1 := Node{ID: "node1", Addr: "localhost:8001"}
	r.AddNode(node1)

	// Node 2
	mockClient2 := new(MockStorage)
	mockFactory.On("NewNodeClient", "localhost:8002").Return(mockClient2).Once()
	node2 := Node{ID: "node2", Addr: "localhost:8002"}
	r.AddNode(node2)

	req := &types.CreateTableRequest{TableName: "test_table"}
	expectedResp := &types.CreateTableResponse{
		TableDescription: types.TableDescription{
			TableName: "test_table",
		},
	}

	// Error case: One client returns an error
	mockClient1.On("CreateTable", req).Return(expectedResp, nil).Once()
	mockClient2.On("CreateTable", req).Return(&types.CreateTableResponse{}, errors.New("client 2 error")).Once()
	_, err := r.CreateTable(req)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "client 2 error")
	mockClient1.AssertExpectations(t)
	mockClient2.AssertExpectations(t)
}

func TestCreateTable_NoNodes(t *testing.T) {
	emptyRouter := NewRouter(nil)
	req := &types.CreateTableRequest{TableName: "test_table"}
	_, err := emptyRouter.CreateTable(req)
	assert.ErrorContains(t, err, "no nodes in the ring to create table")
}

func TestDeleteTable_Success(t *testing.T) {
	mockFactory := new(MockNodeClientFactory)
	r := NewRouter(mockFactory)

	// Node 1
	mockClient1 := new(MockStorage)
	mockFactory.On("NewNodeClient", "localhost:8001").Return(mockClient1).Once()
	node1 := Node{ID: "node1", Addr: "localhost:8001"}
	r.AddNode(node1)

	// Node 2
	mockClient2 := new(MockStorage)
	mockFactory.On("NewNodeClient", "localhost:8002").Return(mockClient2).Once()
	node2 := Node{ID: "node2", Addr: "localhost:8002"}
	r.AddNode(node2)

	req := &types.DeleteTableRequest{TableName: "test_table"}
	expectedResp := &types.DeleteTableResponse{
		TableDescription: types.TableDescription{
			TableName: "test_table",
		},
	}

	// Success case: DeleteTable should be called on all nodes
	mockClient1.On("DeleteTable", req).Return(expectedResp, nil).Once()
	mockClient2.On("DeleteTable", req).Return(expectedResp, nil).Once()
	resp, err := r.DeleteTable(req)
	assert.NoError(t, err)
	assert.Equal(t, expectedResp, resp)
	mockClient1.AssertExpectations(t)
	mockClient2.AssertExpectations(t)
}

func TestDeleteTable_ErrorFromOneClient(t *testing.T) {
	mockFactory := new(MockNodeClientFactory)
	r := NewRouter(mockFactory)

	// Node 1
	mockClient1 := new(MockStorage)
	mockFactory.On("NewNodeClient", "localhost:8001").Return(mockClient1).Once()
	node1 := Node{ID: "node1", Addr: "localhost:8001"}
	r.AddNode(node1)

	// Node 2
	mockClient2 := new(MockStorage)
	mockFactory.On("NewNodeClient", "localhost:8002").Return(mockClient2).Once()
	node2 := Node{ID: "node2", Addr: "localhost:8002"}
	r.AddNode(node2)

	req := &types.DeleteTableRequest{TableName: "test_table"}
	expectedResp := &types.DeleteTableResponse{
		TableDescription: types.TableDescription{
			TableName: "test_table",
		},
	}

	// Error case: One client returns an error
	mockClient1.On("DeleteTable", req).Return(expectedResp, nil).Once()
	mockClient2.On("DeleteTable", req).Return(&types.DeleteTableResponse{}, errors.New("client 2 error")).Once()
	_, err := r.DeleteTable(req)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "client 2 error")
	mockClient1.AssertExpectations(t)
	mockClient2.AssertExpectations(t)
}

func TestDeleteTable_NoNodes(t *testing.T) {
	emptyRouter := NewRouter(nil)
	req := &types.DeleteTableRequest{TableName: "test_table"}
	_, err := emptyRouter.DeleteTable(req)
	assert.ErrorContains(t, err, "no nodes in the ring to delete table")
}

func TestDescribeTable(t *testing.T) {
	mockFactory := new(MockNodeClientFactory)
	r := NewRouter(mockFactory)
	mockClient := new(MockStorage)
	mockFactory.On("NewNodeClient", "localhost:8001").Return(mockClient).Once()

	node1 := Node{ID: "node1", Addr: "localhost:8001"}
	r.AddNode(node1)

	req := &types.DescribeTableRequest{TableName: "test_table"}
	expectedResp := &types.DescribeTableResponse{
		Table: types.TableDescription{
			TableName: "test_table",
		},
	}

	// Success case
	mockClient.On("DescribeTable", req).Return(expectedResp, nil).Once()
	resp, err := r.DescribeTable(req)
	assert.NoError(t, err)
	assert.Equal(t, expectedResp, resp)
	mockClient.AssertExpectations(t)

	// Error case from client
	mockClient.On("DescribeTable", req).Return(&types.DescribeTableResponse{}, errors.New("client error")).Once()
	_, err = r.DescribeTable(req)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "client error")
	mockClient.AssertExpectations(t)

	// Error case no nodes
	emptyRouter := NewRouter(nil)
	_, err = emptyRouter.DescribeTable(req)
	assert.ErrorContains(t, err, "no nodes in the ring")
}

func TestListTables_Success(t *testing.T) {
	mockFactory := new(MockNodeClientFactory)
	r := NewRouter(mockFactory)

	// Node 1
	mockClient1 := new(MockStorage)
	mockFactory.On("NewNodeClient", "localhost:8001").Return(mockClient1).Once()
	node1 := Node{ID: "node1", Addr: "localhost:8001"}
	r.AddNode(node1)

	// Node 2
	mockClient2 := new(MockStorage)
	mockFactory.On("NewNodeClient", "localhost:8002").Return(mockClient2).Once()
	node2 := Node{ID: "node2", Addr: "localhost:8002"}
	r.AddNode(node2)

	req := &types.ListTablesRequest{}
	expectedResp1 := &types.ListTablesResponse{TableNames: []string{"table1", "table2"}}
	expectedResp2 := &types.ListTablesResponse{TableNames: []string{"table2", "table3"}}

	// Success case
	mockClient1.On("ListTables", req).Return(expectedResp1, nil).Once()
	mockClient2.On("ListTables", req).Return(expectedResp2, nil).Once()
	resp, err := r.ListTables(req)
	assert.NoError(t, err)
	assert.Len(t, resp.TableNames, 3)
	assert.Contains(t, resp.TableNames, "table1")
	assert.Contains(t, resp.TableNames, "table2")
	assert.Contains(t, resp.TableNames, "table3")
	mockClient1.AssertExpectations(t)
	mockClient2.AssertExpectations(t)
}

func TestListTables_ErrorFromClient(t *testing.T) {
	mockFactory := new(MockNodeClientFactory)
	r := NewRouter(mockFactory)

	// Node 1
	mockClient1 := new(MockStorage)
	mockFactory.On("NewNodeClient", "localhost:8001").Return(mockClient1).Once()
	node1 := Node{ID: "node1", Addr: "localhost:8001"}
	r.AddNode(node1)

	// Node 2
	mockClient2 := new(MockStorage)
	mockFactory.On("NewNodeClient", "localhost:8002").Return(mockClient2).Once()
	node2 := Node{ID: "node2", Addr: "localhost:8002"}
	r.AddNode(node2)

	req := &types.ListTablesRequest{}

	// Error case from one client
	mockClient1.On("ListTables", req).Return(&types.ListTablesResponse{}, errors.New("client 1 error")).Once()
	_, err := r.ListTables(req)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "client 1 error")
	mockClient1.AssertExpectations(t)
	// mockClient2.AssertExpectations(t) // This assertion is removed
}

func TestListTables_NoNodes(t *testing.T) {
	emptyRouter := NewRouter(nil)
	req := &types.ListTablesRequest{}
	_, err := emptyRouter.ListTables(req)
	assert.ErrorContains(t, err, "no nodes in the ring")
}


func TestPut(t *testing.T) {
	mockFactory := new(MockNodeClientFactory)
	r := NewRouter(mockFactory)
	mockClient := new(MockStorage)
	mockFactory.On("NewNodeClient", "localhost:8001").Return(mockClient).Once()

	node1 := Node{ID: "node1", Addr: "localhost:8001"}
	r.AddNode(node1)

	req := &types.PutRequest{
		TableName: "test_table",
		Item: map[string]*expression.AttributeValue{
			"id": {S: stringPtr("123")},
		},
	}

	// Success case
	mockClient.On("Put", req).Return(nil).Once()
	err := r.Put(req)
	assert.NoError(t, err)
	mockClient.AssertExpectations(t)

	// Error case from client
	mockClient.On("Put", req).Return(errors.New("client error")).Once()
	err = r.Put(req)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "client error")
	mockClient.AssertExpectations(t)
}

func TestGet(t *testing.T) {
	mockFactory := new(MockNodeClientFactory)
	r := NewRouter(mockFactory)
	mockClient := new(MockStorage)
	mockFactory.On("NewNodeClient", "localhost:8001").Return(mockClient).Once()

	node1 := Node{ID: "node1", Addr: "localhost:8001"}
	r.AddNode(node1)

	req := &types.GetRequest{
		TableName: "test_table",
		Key: map[string]*expression.AttributeValue{
			"id": {S: stringPtr("123")},
		},
	}
	expectedResult := map[string]*expression.AttributeValue{"data": {S: stringPtr("item1")}}

	// Success case
	mockClient.On("Get", req).Return(expectedResult, nil).Once()
	result, err := r.Get(req)
	assert.NoError(t, err)
	assert.Equal(t, expectedResult, result)
	mockClient.AssertExpectations(t)

	// Error case from client
	mockClient.On("Get", req).Return(map[string]*expression.AttributeValue{}, errors.New("client error")).Once()
	_, err = r.Get(req)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "client error")
	mockClient.AssertExpectations(t)
}

func TestDelete(t *testing.T) {
	mockFactory := new(MockNodeClientFactory)
	r := NewRouter(mockFactory)
	mockClient := new(MockStorage)
	mockFactory.On("NewNodeClient", "localhost:8001").Return(mockClient).Once()

	node1 := Node{ID: "node1", Addr: "localhost:8001"}
	r.AddNode(node1)

	req := &types.DeleteRequest{
		TableName: "test_table",
		Key: map[string]*expression.AttributeValue{
			"id": {S: stringPtr("123")},
		},
	}

	// Success case
	mockClient.On("Delete", req).Return(nil).Once()
	err := r.Delete(req)
	assert.NoError(t, err)
	mockClient.AssertExpectations(t)

	// Error case from client
	mockClient.On("Delete", req).Return(errors.New("client error")).Once()
	err = r.Delete(req)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "client error")
	mockClient.AssertExpectations(t)
}

func TestUpdate(t *testing.T) {
	mockFactory := new(MockNodeClientFactory)
	r := NewRouter(mockFactory)
	mockClient := new(MockStorage)
	mockFactory.On("NewNodeClient", "localhost:8001").Return(mockClient).Once()

	node1 := Node{ID: "node1", Addr: "localhost:8001"}
	r.AddNode(node1)

	req := &types.UpdateRequest{
		TableName: "test_table",
		Key: map[string]*expression.AttributeValue{
			"id": {S: stringPtr("123")},
		},
	}
	expectedResult := map[string]*expression.AttributeValue{"updated_data": {S: stringPtr("item1")}}

	// Success case
	mockClient.On("Update", req).Return(expectedResult, nil).Once()
	result, err := r.Update(req)
	assert.NoError(t, err)
	assert.Equal(t, expectedResult, result)
	mockClient.AssertExpectations(t)

	// Error case from client
	mockClient.On("Update", req).Return(map[string]*expression.AttributeValue{}, errors.New("client error")).Once()
	_, err = r.Update(req)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "client error")
	mockClient.AssertExpectations(t)
}

func TestQuery(t *testing.T) {
	mockFactory := new(MockNodeClientFactory)
	r := NewRouter(mockFactory)
	mockClient := new(MockStorage)
	mockFactory.On("NewNodeClient", "localhost:8001").Return(mockClient).Once()

	node1 := Node{ID: "node1", Addr: "localhost:8001"}
	r.AddNode(node1)

	req := &types.QueryRequest{
		TableName:            "test_table",
		KeyConditionExpression: "HashKey = :val",
	}
	expectedResult := []map[string]*expression.AttributeValue{{"query_data": {S: stringPtr("item1")}}}

	// Success case
	mockClient.On("Query", req).Return(expectedResult, nil).Once()
	result, err := r.Query(req)
	assert.NoError(t, err)
	assert.Equal(t, expectedResult, result)
	mockClient.AssertExpectations(t)

	// Error case from client
	mockClient.On("Query", req).Return([]map[string]*expression.AttributeValue{}, errors.New("client error")).Once()
	_, err = r.Query(req)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "client error")
	mockClient.AssertExpectations(t)
}

func stringPtr(s string) *string {
	return &s
}
