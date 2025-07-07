package bbolt_test

import (
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"zagreb/pkg/expression"
	"zagreb/pkg/storage/bbolt"
	"zagreb/pkg/types"
)

func TestBBoltStorage_CreateTable(t *testing.T) {
	f, err := ioutil.TempFile("", "bbolt.db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())

	s, err := bbolt.NewBBoltStorage(f.Name())
	if err != nil {
		t.Fatal(err)
	}

	req := &types.CreateTableRequest{
		TableName: "test-table",
		AttributeDefinitions: []*types.AttributeDefinition{
			{AttributeName: "id", AttributeType: "S"},
		},
		KeySchema: []*types.KeySchemaElement{
			{AttributeName: "id", KeyType: "HASH"},
		},
	}

	if err := s.CreateTable(req); err != nil {
		t.Fatal(err)
	}
}

func TestBBoltStorage_PutGet(t *testing.T) {
	f, err := ioutil.TempFile("", "bbolt.db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())

	s, err := bbolt.NewBBoltStorage(f.Name())
	if err != nil {
		t.Fatal(err)
	}

	createReq := &types.CreateTableRequest{
		TableName: "test-table",
		AttributeDefinitions: []*types.AttributeDefinition{
			{AttributeName: "id", AttributeType: "S"},
			{AttributeName: "name", AttributeType: "S"},
		},
		KeySchema: []*types.KeySchemaElement{
			{AttributeName: "id", KeyType: "HASH"},
		},
	}

	if err := s.CreateTable(createReq); err != nil {
		t.Fatal(err)
	}
	putReq := &types.PutRequest{
		TableName: "test-table",
		Item: map[string]*expression.AttributeValue{
			"id":   {S: stringPtr("123")},
			"name": {S: stringPtr("test-name")},
		},
	}

	if err := s.Put(putReq); err != nil {
		t.Fatal(err)
	}

	getReq := &types.GetRequest{
		TableName: "test-table",
		Key: map[string]*expression.AttributeValue{
			"id": {S: stringPtr("123")},
		},
	}

	item, err := s.Get(getReq)
	if err != nil {
		t.Fatal(err)
	}

	if item == nil {
		t.Fatal("expected item, got nil")
	}

	if *item["id"].S != "123" {
		t.Errorf("expected id to be '123', got '%s'", *item["id"].S)
	}

	if *item["name"].S != "test-name" {
		t.Errorf("expected name to be 'test-name', got '%s'", *item["name"].S)
	}
}

func TestBBoltStorage_PutGet_CompositeKey(t *testing.T) {
	f, err := ioutil.TempFile("", "bbolt.db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())

	s, err := bbolt.NewBBoltStorage(f.Name())
	if err != nil {
		t.Fatal(err)
	}

	createReq := &types.CreateTableRequest{
		TableName: "test-table",
		AttributeDefinitions: []*types.AttributeDefinition{
			{AttributeName: "id", AttributeType: "S"},
			{AttributeName: "timestamp", AttributeType: "N"},
			{AttributeName: "name", AttributeType: "S"},
		},
		KeySchema: []*types.KeySchemaElement{
			{AttributeName: "id", KeyType: "HASH"},
			{AttributeName: "timestamp", KeyType: "RANGE"},
		},
	}

	if err := s.CreateTable(createReq); err != nil {
		t.Fatal(err)
	}

	putReq := &types.PutRequest{
		TableName: "test-table",
		Item: map[string]*expression.AttributeValue{
			"id":        {S: stringPtr("123")},
			"timestamp": {N: stringPtr("1678886400")},
			"name":      {S: stringPtr("test-name")},
		},
	}

	if err := s.Put(putReq); err != nil {
		t.Fatal(err)
	}

	getReq := &types.GetRequest{
		TableName: "test-table",
		Key: map[string]*expression.AttributeValue{
			"id":        {S: stringPtr("123")},
			"timestamp": {N: stringPtr("1678886400")},
		},
	}

	item, err := s.Get(getReq)
	if err != nil {
		t.Fatal(err)
	}

	if item == nil {
		t.Fatal("expected item, got nil")
	}

	if *item["id"].S != "123" {
		t.Errorf("expected id to be '123', got '%s'", *item["id"].S)
	}

	if *item["timestamp"].N != "1678886400" {
		t.Errorf("expected timestamp to be '1678886400', got '%s'", *item["timestamp"].N)
	}

	if *item["name"].S != "test-name" {
		t.Errorf("expected name to be 'test-name', got '%s'", *item["name"].S)
	}
}

func TestBBoltStorage_Delete(t *testing.T) {
	f, err := ioutil.TempFile("", "bbolt.db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())

	s, err := bbolt.NewBBoltStorage(f.Name())
	if err != nil {
		t.Fatal(err)
	}

	createReq := &types.CreateTableRequest{
		TableName: "test-table",
		AttributeDefinitions: []*types.AttributeDefinition{
			{AttributeName: "id", AttributeType: "S"},
			{AttributeName: "name", AttributeType: "S"},
		},
		KeySchema: []*types.KeySchemaElement{
			{AttributeName: "id", KeyType: "HASH"},
		},
	}

	if err := s.CreateTable(createReq); err != nil {
		t.Fatal(err)
	}

	putReq := &types.PutRequest{
		TableName: "test-table",
		Item: map[string]*expression.AttributeValue{
			"id":   {S: stringPtr("123")},
			"name": {S: stringPtr("test-name")},
		},
	}

	if err := s.Put(putReq); err != nil {
		t.Fatal(err)
	}

	deleteReq := &types.DeleteRequest{
		TableName: "test-table",
		Key: map[string]*expression.AttributeValue{
			"id": {S: stringPtr("123")},
		},
	}

	if err := s.Delete(deleteReq); err != nil {
		t.Fatal(err)
	}

	getReq := &types.GetRequest{
		TableName: "test-table",
		Key: map[string]*expression.AttributeValue{
			"id": {S: stringPtr("123")},
		},
	}

	item, err := s.Get(getReq)
	if err != nil {
		t.Fatal(err)
	}

	if item != nil {
		t.Fatal("expected item to be nil, got item")
	}
}

func TestBBoltStorage_Update(t *testing.T) {
	f, err := ioutil.TempFile("", "bbolt.db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())

	s, err := bbolt.NewBBoltStorage(f.Name())
	if err != nil {
		t.Fatal(err)
	}

	createReq := &types.CreateTableRequest{
		TableName: "test-table",
		AttributeDefinitions: []*types.AttributeDefinition{
			{AttributeName: "id", AttributeType: "S"},
			{AttributeName: "name", AttributeType: "S"},
		},
		KeySchema: []*types.KeySchemaElement{
			{AttributeName: "id", KeyType: "HASH"},
		},
	}

	if err := s.CreateTable(createReq); err != nil {
		t.Fatal(err)
	}

	putReq := &types.PutRequest{
		TableName: "test-table",
		Item: map[string]*expression.AttributeValue{
			"id":   {S: stringPtr("123")},
			"name": {S: stringPtr("test-name")},
		},
	}

	if err := s.Put(putReq); err != nil {
		t.Fatal(err)
	}

	updateReq := &types.UpdateRequest{
		TableName: "test-table",
		Key: map[string]*expression.AttributeValue{
			"id": {S: stringPtr("123")},
		},
		UpdateExpression: "SET name = new-name",
	}

	updatedItem, err := s.Update(updateReq)
	if err != nil {
		t.Fatal(err)
	}

	if *updatedItem["name"].S != "new-name" {
		t.Errorf("expected name to be 'new-name', got '%s'", *updatedItem["name"].S)
	}
}

func TestBBoltStorage_Query(t *testing.T) {
	f, err := ioutil.TempFile("", "bbolt.db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())

	s, err := bbolt.NewBBoltStorage(f.Name())
	if err != nil {
		t.Fatal(err)
	}

	createReq := &types.CreateTableRequest{
		TableName: "query-test-table",
		AttributeDefinitions: []*types.AttributeDefinition{
			{AttributeName: "id", AttributeType: "S"},
			{AttributeName: "name", AttributeType: "S"},
		},
		KeySchema: []*types.KeySchemaElement{
			{AttributeName: "id", KeyType: "HASH"},
		},
	}

	if err := s.CreateTable(createReq); err != nil {
		t.Fatal(err)
	}

	putReq1 := &types.PutRequest{
		TableName: "query-test-table",
		Item: map[string]*expression.AttributeValue{
			"id":   {S: stringPtr("123")},
			"name": {S: stringPtr("test-name-1")},
		},
	}

	if err := s.Put(putReq1); err != nil {
		t.Fatal(err)
	}

	queryReq := &types.QueryRequest{
		TableName:              "query-test-table",
		KeyConditionExpression: "id = :id",
		ExpressionAttributeValues: map[string]*expression.AttributeValue{
			":id": {S: stringPtr("123")},
		},
	}

	items, err := s.Query(queryReq)
	if err != nil {
		t.Fatal(err)
	}

	if len(items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(items))
	}
}

func TestBBoltStorage_Query_Validation(t *testing.T) {
	f, err := ioutil.TempFile("", "bbolt.db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())

	s, err := bbolt.NewBBoltStorage(f.Name())
	if err != nil {
		t.Fatal(err)
	}

	createReq := &types.CreateTableRequest{
		TableName: "test-table",
		AttributeDefinitions: []*types.AttributeDefinition{
			{AttributeName: "id", AttributeType: "S"},
			{AttributeName: "age", AttributeType: "N"},
		},
		KeySchema: []*types.KeySchemaElement{
			{AttributeName: "id", KeyType: "HASH"},
		},
	}

	if err := s.CreateTable(createReq); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name                   string
		keyConditionExpression string
		expectedError          string
	}{
		{
			name:                   "Invalid format - missing operator",
			keyConditionExpression: "id 123",
			expectedError:          "invalid KeyConditionExpression format: expected 'attributeName = value'",
		},
		{
			name:                   "Invalid format - too many parts",
			keyConditionExpression: "id = 123 extra",
			expectedError:          "invalid KeyConditionExpression format: expected 'attributeName = value'",
		},
		{
			name:                   "Attribute name does not match hash key",
			keyConditionExpression: "age = 30",
			expectedError:          "KeyConditionExpression must use the hash key 'id', but got 'age'",
		},
		{
			name:                   "Value type mismatch",
			keyConditionExpression: "id = :id", // id is S, but 123 is N
			expectedError:          "invalid type for hash key 'id': expected S, got N",
		},
		{
			name:                   "Valid expression",
			keyConditionExpression: "id = :id", // id is S, and "123" is S
			expectedError:          "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			queryReq := &types.QueryRequest{
				TableName:              "test-table",
				KeyConditionExpression: tt.keyConditionExpression,
				ExpressionAttributeValues: map[string]*expression.AttributeValue{
					":id": {N: stringPtr("123")},
				},
			}
			if tt.name == "Valid expression" {
				queryReq.ExpressionAttributeValues = map[string]*expression.AttributeValue{
					":id": {S: stringPtr("123")},
				}
			}

			_, err := s.Query(queryReq)
			if tt.expectedError == "" {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
			} else {
				if err == nil || !strings.Contains(err.Error(), tt.expectedError) {
					t.Fatalf("expected error containing '%s', got %v", tt.expectedError, err)
				}
			}
		})
	}
}

func stringPtr(s string) *string {
	return &s
}