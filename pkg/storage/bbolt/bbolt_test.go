package bbolt_test

import (
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

	_, err = s.CreateTable(req)
	if err != nil {
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

	_, err = s.CreateTable(createReq)
	if err != nil {
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

	_, err = s.CreateTable(createReq)
	if err != nil {
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

	_, err = s.CreateTable(createReq)
	if err != nil {
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

	_, err = s.CreateTable(createReq)
	if err != nil {
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
		UpdateExpression: "SET name = :newName",
		ExpressionAttributeValues: map[string]*expression.AttributeValue{
			":newName": {S: stringPtr("new-name")},
		},
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

	_, err = s.CreateTable(createReq)
	if err != nil {
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

	_, err = s.CreateTable(createReq)
	if err != nil {
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

func TestDeleteTable(t *testing.T) {
	dbPath := "test_delete_table.db"
	s, err := bbolt.NewBBoltStorage(dbPath)
	require.NoError(t, err)
	defer os.Remove(dbPath)

	// Create a table
	createTableReq := &types.CreateTableRequest{
		TableName: "TestTable",
		KeySchema: []*types.KeySchemaElement{
			{AttributeName: "ID", KeyType: "HASH"},
		},
		AttributeDefinitions: []*types.AttributeDefinition{
			{AttributeName: "ID", AttributeType: "S"},
		},
	}
	_, err = s.CreateTable(createTableReq)
	require.NoError(t, err)

	// Verify table exists
	describeTableReq := &types.DescribeTableRequest{TableName: "TestTable"}
	_, err = s.DescribeTable(describeTableReq)
	require.NoError(t, err)

	// Delete the table
	deleteTableReq := &types.DeleteTableRequest{TableName: "TestTable"}
	deleteResp, err := s.DeleteTable(deleteTableReq)
	require.NoError(t, err)
	assert.Equal(t, "TestTable", deleteResp.TableDescription.TableName)

	// Verify table no longer exists
	_, err = s.DescribeTable(describeTableReq)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "table not found")

	// Try to delete a non-existent table
	deleteTableReq = &types.DeleteTableRequest{TableName: "NonExistentTable"}
	_, err = s.DeleteTable(deleteTableReq)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "table not found")
}

func TestDescribeTable(t *testing.T) {
	dbPath := "test_describe_table.db"
	s, err := bbolt.NewBBoltStorage(dbPath)
	require.NoError(t, err)
	defer os.Remove(dbPath)

	// Create a table
	createTableReq := &types.CreateTableRequest{
		TableName: "MyTable",
		KeySchema: []*types.KeySchemaElement{
			{AttributeName: "PK", KeyType: "HASH"},
			{AttributeName: "SK", KeyType: "RANGE"},
		},
		AttributeDefinitions: []*types.AttributeDefinition{
			{AttributeName: "PK", AttributeType: "S"},
			{AttributeName: "SK", AttributeType: "N"},
		},
	}
	_, err = s.CreateTable(createTableReq)
	require.NoError(t, err)

	// Describe the table
	describeTableReq := &types.DescribeTableRequest{TableName: "MyTable"}
	resp, err := s.DescribeTable(describeTableReq)
	require.NoError(t, err)
	assert.Equal(t, "MyTable", resp.Table.TableName)
	assert.Len(t, resp.Table.KeySchema, 2)
	assert.Len(t, resp.Table.AttributeDefinitions, 2)
	assert.Equal(t, "PK", resp.Table.KeySchema[0].AttributeName)
	assert.Equal(t, "HASH", resp.Table.KeySchema[0].KeyType)
	assert.Equal(t, "SK", resp.Table.KeySchema[1].AttributeName)
	assert.Equal(t, "RANGE", resp.Table.KeySchema[1].KeyType)
	assert.Equal(t, "PK", resp.Table.AttributeDefinitions[0].AttributeName)
	assert.Equal(t, "S", resp.Table.AttributeDefinitions[0].AttributeType)
	assert.Equal(t, "SK", resp.Table.AttributeDefinitions[1].AttributeName)
	assert.Equal(t, "N", resp.Table.AttributeDefinitions[1].AttributeType)

	// Describe a non-existent table
	describeTableReq = &types.DescribeTableRequest{TableName: "NonExistentTable"}
	_, err = s.DescribeTable(describeTableReq)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "table not found")
}

func TestListTables(t *testing.T) {
	dbPath := "test_list_tables.db"
	s, err := bbolt.NewBBoltStorage(dbPath)
	require.NoError(t, err)
	defer os.Remove(dbPath)

	// Initially, no tables
	listTablesReq := &types.ListTablesRequest{}
	resp, err := s.ListTables(listTablesReq)
	require.NoError(t, err)
	assert.Empty(t, resp.TableNames)

	// Create a few tables
	table1Req := &types.CreateTableRequest{TableName: "Table1"}
	_, err = s.CreateTable(table1Req)
	require.NoError(t, err)

	table2Req := &types.CreateTableRequest{TableName: "Table2"}
	_, err = s.CreateTable(table2Req)
	require.NoError(t, err)

	table3Req := &types.CreateTableRequest{TableName: "Table3"}
	_, err = s.CreateTable(table3Req)
	require.NoError(t, err)

	// List tables
	resp, err = s.ListTables(listTablesReq)
	require.NoError(t, err)
	assert.Len(t, resp.TableNames, 3)
	assert.Contains(t, resp.TableNames, "Table1")
	assert.Contains(t, resp.TableNames, "Table2")
	assert.Contains(t, resp.TableNames, "Table3")

	// Delete one table and list again
	deleteTableReq := &types.DeleteTableRequest{TableName: "Table2"}
	_, err = s.DeleteTable(deleteTableReq)
	require.NoError(t, err)

	resp, err = s.ListTables(listTablesReq)
	require.NoError(t, err)
	assert.Len(t, resp.TableNames, 2)
	assert.Contains(t, resp.TableNames, "Table1")
	assert.NotContains(t, resp.TableNames, "Table2")
	assert.Contains(t, resp.TableNames, "Table3")
}

func TestBBoltStorage_Scan(t *testing.T) {
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
		TableName: "scan-test-table",
		AttributeDefinitions: []*types.AttributeDefinition{
			{AttributeName: "id", AttributeType: "S"},
			{AttributeName: "data", AttributeType: "S"},
		},
		KeySchema: []*types.KeySchemaElement{
			{AttributeName: "id", KeyType: "HASH"},
		},
	}

	_, err = s.CreateTable(createReq)
	if err != nil {
		t.Fatal(err)
	}

	// Put some items
	itemsToPut := []map[string]*expression.AttributeValue{
		{
			"id":   {S: stringPtr("item1")},
			"data": {S: stringPtr("data1")},
		},
		{
			"id":   {S: stringPtr("item2")},
			"data": {S: stringPtr("data2")},
		},
		{
			"id":   {S: stringPtr("item3")},
			"data": {S: stringPtr("data3")},
		},
		{
			"id":   {S: stringPtr("item4")},
			"data": {S: stringPtr("data4")},
		},
		{
			"id":   {S: stringPtr("item5")},
			"data": {S: stringPtr("data5")},
		},
	}

	for _, item := range itemsToPut {
		putReq := &types.PutRequest{
			TableName: "scan-test-table",
			Item:      item,
		}
		if err := s.Put(putReq); err != nil {
			t.Fatal(err)
		}
	}

	// Test full scan (no pagination)
	scanReq := &types.ScanRequest{
		TableName: "scan-test-table",
	}

	resp, err := s.Scan(scanReq)
	if err != nil {
		t.Fatal(err)
	}

	assert.Len(t, resp.Items, len(itemsToPut), "Expected same number of scanned items as put items")
	assert.Equal(t, len(itemsToPut), resp.ScannedCount, "Expected ScannedCount to match total items")
	assert.Nil(t, resp.LastEvaluatedKey, "Expected LastEvaluatedKey to be nil for full scan")

	// Verify that all put items are present in the scanned items
	foundCount := 0
	for _, putItem := range itemsToPut {
		for _, scannedItem := range resp.Items {
			if *putItem["id"].S == *scannedItem["id"].S && *putItem["data"].S == *scannedItem["data"].S {
				foundCount++
				break
			}
		}
	}
	assert.Equal(t, len(itemsToPut), foundCount, "Not all put items were found in scan results")

	// Test scanning a non-existent table
	scanReq.TableName = "non-existent-table"
	resp, err = s.Scan(scanReq)
	assert.NoError(t, err)
	assert.Empty(t, resp.Items, "Expected empty slice for non-existent table scan")
	assert.Equal(t, 0, resp.ScannedCount, "Expected ScannedCount to be 0 for non-existent table scan")
	assert.Nil(t, resp.LastEvaluatedKey, "Expected LastEvaluatedKey to be nil for non-existent table scan")

	// Test paginated scan
	scanReq.TableName = "scan-test-table"
	limit := 2
	scanReq.Limit = &limit
	scanReq.ExclusiveStartKey = nil // Start from beginning

	var allScannedItems []map[string]*expression.AttributeValue
	for {
		resp, err := s.Scan(scanReq)
		require.NoError(t, err)

		allScannedItems = append(allScannedItems, resp.Items...)

		if resp.LastEvaluatedKey == nil {
			break
		}
		scanReq.ExclusiveStartKey = resp.LastEvaluatedKey
	}

	assert.Len(t, allScannedItems, len(itemsToPut), "Expected all items after paginated scan")

	// Verify all items are present after pagination
	foundCount = 0
	for _, putItem := range itemsToPut {
		for _, scannedItem := range allScannedItems {
			if *putItem["id"].S == *scannedItem["id"].S && *putItem["data"].S == *scannedItem["data"].S {
				foundCount++
				break
			}
		}
	}
	assert.Equal(t, len(itemsToPut), foundCount, "Not all put items were found in paginated scan results")
}

func stringPtr(s string) *string {
	return &s
}
