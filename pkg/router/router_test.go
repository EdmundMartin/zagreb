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

func (m *MockStorage) CreateTable(req *types.CreateTableRequest) error {
	args := m.Called(req)
	return args.Error(0)
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

func TestCreateTable(t *testing.T) {
	mockFactory := new(MockNodeClientFactory)
	r := NewRouter(mockFactory)
	mockClient := new(MockStorage)
	mockFactory.On("NewNodeClient", "localhost:8001").Return(mockClient).Once()

	node1 := Node{ID: "node1", Addr: "localhost:8001"}
	r.AddNode(node1)

	req := &types.CreateTableRequest{TableName: "test_table"}

	// Success case
	mockClient.On("CreateTable", req).Return(nil).Once()
	err := r.CreateTable(req)
	assert.NoError(t, err)
	mockClient.AssertExpectations(t)

	// Error case from client
	mockClient.On("CreateTable", req).Return(errors.New("client error")).Once()
	err = r.CreateTable(req)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "client error")
	mockClient.AssertExpectations(t)

	// Error case no nodes
	emptyRouter := NewRouter(nil)
	err = emptyRouter.CreateTable(req)
	assert.ErrorContains(t, err, "no nodes in the ring")
}

func TestPut(t *testing.T) {
	mockFactory := new(MockNodeClientFactory)
	r := NewRouter(mockFactory)
	mockClient := new(MockStorage)
	mockFactory.On("NewNodeClient", "localhost:8001").Return(mockClient).Once()

	node1 := Node{ID: "node1", Addr: "localhost:8001"}
	r.AddNode(node1)

	hashKeyVal := "item1"
	req := &types.PutRequest{
		TableName: "test_table",
		Item: map[string]*expression.AttributeValue{
			"test_table": {
				M: map[string]*expression.AttributeValue{
					"test_table": {S: &hashKeyVal},
				},
			},
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

	hashKeyVal := "item1"
	req := &types.GetRequest{
		TableName: "test_table",
		Key: map[string]*expression.AttributeValue{
			"test_table": {S: &hashKeyVal},
		},
	}
	expectedResult := map[string]*expression.AttributeValue{"data": {S: &hashKeyVal}}

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

	hashKeyVal := "item1"
	req := &types.DeleteRequest{
		TableName: "test_table",
		Key: map[string]*expression.AttributeValue{
			"test_table": {S: &hashKeyVal},
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

	hashKeyVal := "item1"
	req := &types.UpdateRequest{
		TableName: "test_table",
		Key: map[string]*expression.AttributeValue{
			"test_table": {S: &hashKeyVal},
		},
	}
	expectedResult := map[string]*expression.AttributeValue{"updated_data": {S: &hashKeyVal}}

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
	expectedResult := []map[string]*expression.AttributeValue{{"query_data": {S: new(string)}}}

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


