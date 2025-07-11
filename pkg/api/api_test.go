package api_test

import (
	"context"
	

	"net/http/httptest"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	awstypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	api "zagreb/pkg/api"
	bbolt "zagreb/pkg/storage/bbolt"
)

func setupTestServer(t *testing.T) (*dynamodb.Client, func()) {
	// Create a temporary bbolt database file
	dbFile, err := os.CreateTemp("", "zagreb-test-*.db")
	if err != nil {
		t.Fatalf("failed to create temp db file: %v", err)
	}
	dbPath := dbFile.Name()
	dbFile.Close()

	// Initialize bbolt storage
	storage, err := bbolt.NewBBoltStorage(dbPath)
	if err != nil {
		t.Fatalf("failed to create bbolt storage: %v", err)
	}

	// Create and start the API server
	server := api.NewServer(storage)
	testServer := httptest.NewServer(server.Router()) // Assuming Router() method is public or accessible

	// Configure AWS SDK to use the test server endpoint
	customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{
			PartitionID:   "aws",
			URL:           testServer.URL,
			SigningRegion: "us-east-1",
		},
		nil
	})

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithEndpointResolverWithOptions(customResolver),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")), // Dummy credentials
		config.WithRegion("us-east-1"),
	)
	if err != nil {
		t.Fatalf("failed to load AWS config: %v", err)
	}

	dbClient := dynamodb.NewFromConfig(cfg)

	cleanup := func() {
		testServer.Close()
		os.Remove(dbPath)
	}

	return dbClient, cleanup
}

func TestCreateTable(t *testing.T) {
	dbClient, cleanup := setupTestServer(t)
	defer cleanup()

	tableName := "TestTable"
	_, err := dbClient.CreateTable(context.TODO(), &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []awstypes.KeySchemaElement{
			{AttributeName: aws.String("ID"), KeyType: awstypes.KeyTypeHash},
		},
		AttributeDefinitions: []awstypes.AttributeDefinition{
			{AttributeName: aws.String("ID"), AttributeType: awstypes.ScalarAttributeTypeS},
		},
		ProvisionedThroughput: &awstypes.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},
	})

	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// You might want to add a DescribeTable call here to verify creation
}

func TestPutGetItem(t *testing.T) {
	dbClient, cleanup := setupTestServer(t)
	defer cleanup()

	tableName := "TestPutGetTable"
	_, err := dbClient.CreateTable(context.TODO(), &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []awstypes.KeySchemaElement{
			{AttributeName: aws.String("ID"), KeyType: awstypes.KeyTypeHash},
			{AttributeName: aws.String("RangeKey"), KeyType: awstypes.KeyTypeRange},
		},
		AttributeDefinitions: []awstypes.AttributeDefinition{
			{AttributeName: aws.String("ID"), AttributeType: awstypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("RangeKey"), AttributeType: awstypes.ScalarAttributeTypeN},
		},
		ProvisionedThroughput: &awstypes.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},
	})
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	item := map[string]awstypes.AttributeValue{
		"ID":       &awstypes.AttributeValueMemberS{Value: "item1"},
		"RangeKey": &awstypes.AttributeValueMemberN{Value: "123"},
		"Data":     &awstypes.AttributeValueMemberS{Value: "some data"},
	}

	_, err = dbClient.PutItem(context.TODO(), &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item:      item,
	})
	if err != nil {
		t.Fatalf("PutItem failed: %v", err)
	}

	getItemOutput, err := dbClient.GetItem(context.TODO(), &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]awstypes.AttributeValue{
			"ID":       &awstypes.AttributeValueMemberS{Value: "item1"},
			"RangeKey": &awstypes.AttributeValueMemberN{Value: "123"},
		},
	})
	if err != nil {
		t.Fatalf("GetItem failed: %v", err)
	}

	if getItemOutput.Item == nil {
		t.Fatal("GetItem returned nil item")
	}

	if v, ok := getItemOutput.Item["Data"]; !ok {
		t.Errorf("expected Data attribute, but not found")
	} else if strVal, ok := v.(*awstypes.AttributeValueMemberS); !ok || strVal.Value != "some data" {
		t.Errorf("expected Data to be 'some data', got %v", v)
	}
}

func TestUpdateItem(t *testing.T) {
	dbClient, cleanup := setupTestServer(t)
	defer cleanup()

	tableName := "TestUpdateTable"
	_, err := dbClient.CreateTable(context.TODO(), &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []awstypes.KeySchemaElement{
			{AttributeName: aws.String("ID"), KeyType: awstypes.KeyTypeHash},
		},
		AttributeDefinitions: []awstypes.AttributeDefinition{
			{AttributeName: aws.String("ID"), AttributeType: awstypes.ScalarAttributeTypeS},
		},
		ProvisionedThroughput: &awstypes.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},
	})
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// Put initial item
	_, err = dbClient.PutItem(context.TODO(), &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]awstypes.AttributeValue{
			"ID":    &awstypes.AttributeValueMemberS{Value: "user1"},
			"Age":   &awstypes.AttributeValueMemberN{Value: "30"},
			"Email": &awstypes.AttributeValueMemberS{Value: "old@example.com"},
		},
	})
	if err != nil {
		t.Fatalf("PutItem failed: %v", err)
	}

	// Test SET operation
	updateOutput, err := dbClient.UpdateItem(context.TODO(), &dynamodb.UpdateItemInput{
		TableName: aws.String(tableName),
		Key: map[string]awstypes.AttributeValue{
			"ID": &awstypes.AttributeValueMemberS{Value: "user1"},
		},
		UpdateExpression: aws.String("SET Email = :newEmail"),
		ExpressionAttributeValues: map[string]awstypes.AttributeValue{
			":newEmail": &awstypes.AttributeValueMemberS{Value: "new@example.com"},
		},
		ReturnValues: awstypes.ReturnValueUpdatedNew,
	})
	if err != nil {
		t.Fatalf("UpdateItem SET failed: %v", err)
	}
	if v, ok := updateOutput.Attributes["Email"]; !ok {
		t.Errorf("expected Email attribute, but not found")
	} else if strVal, ok := v.(*awstypes.AttributeValueMemberS); !ok || strVal.Value != "new@example.com" {
		t.Errorf("expected Email to be 'new@example.com', got %v", v)
	}

	// Test ADD operation
	updateOutput, err = dbClient.UpdateItem(context.TODO(), &dynamodb.UpdateItemInput{
		TableName: aws.String(tableName),
		Key: map[string]awstypes.AttributeValue{
			"ID": &awstypes.AttributeValueMemberS{Value: "user1"},
		},
		UpdateExpression: aws.String("ADD Age :ageIncrement"),
		ExpressionAttributeValues: map[string]awstypes.AttributeValue{
			":ageIncrement": &awstypes.AttributeValueMemberN{Value: "5"},
		},
		ReturnValues: awstypes.ReturnValueUpdatedNew,
	})
	if err != nil {
		t.Fatalf("UpdateItem ADD failed: %v", err)
	}
	if v, ok := updateOutput.Attributes["Age"]; !ok {
		t.Errorf("expected Age attribute, but not found")
	} else if numVal, ok := v.(*awstypes.AttributeValueMemberN); !ok || numVal.Value != "35" {
		t.Errorf("expected Age to be '35', got %v", v)
	}

	// Test REMOVE operation
	updateOutput, err = dbClient.UpdateItem(context.TODO(), &dynamodb.UpdateItemInput{
		TableName: aws.String(tableName),
		Key: map[string]awstypes.AttributeValue{
			"ID": &awstypes.AttributeValueMemberS{Value: "user1"},
		},
		UpdateExpression: aws.String("REMOVE Email"),
		ReturnValues: awstypes.ReturnValueUpdatedNew,
	})
	if err != nil {
		t.Fatalf("UpdateItem REMOVE failed: %v", err)
	}
	if _, ok := updateOutput.Attributes["Email"]; ok {
		t.Errorf("expected Email to be removed, but it still exists")
	}

	// Test DELETE operation (for scalar attributes, it's similar to REMOVE in our simplified impl)
	// First, put an item with a boolean attribute to test DELETE on it
	_, err = dbClient.PutItem(context.TODO(), &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]awstypes.AttributeValue{
			"ID":     &awstypes.AttributeValueMemberS{Value: "user2"},
			"Active": &awstypes.AttributeValueMemberBOOL{Value: true},
		},
	})
	if err != nil {
		t.Fatalf("PutItem for DELETE test failed: %v", err)
	}

	updateOutput, err = dbClient.UpdateItem(context.TODO(), &dynamodb.UpdateItemInput{
		TableName: aws.String(tableName),
		Key: map[string]awstypes.AttributeValue{
			"ID": &awstypes.AttributeValueMemberS{Value: "user2"},
		},
		UpdateExpression: aws.String("DELETE Active"),
		ReturnValues: awstypes.ReturnValueUpdatedNew,
	})
	if err != nil {
		t.Fatalf("UpdateItem DELETE failed: %v", err)
	}
	if _, ok := updateOutput.Attributes["Active"]; ok {
		t.Errorf("expected Active to be deleted, but it still exists")
	}
}

func TestDeleteItem(t *testing.T) {
	dbClient, cleanup := setupTestServer(t)
	defer cleanup()

	tableName := "TestDeleteTable"
	_, err := dbClient.CreateTable(context.TODO(), &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []awstypes.KeySchemaElement{
			{AttributeName: aws.String("ID"), KeyType: awstypes.KeyTypeHash},
		},
		AttributeDefinitions: []awstypes.AttributeDefinition{
			{AttributeName: aws.String("ID"), AttributeType: awstypes.ScalarAttributeTypeS},
		},
		ProvisionedThroughput: &awstypes.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},
	})
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	item := map[string]awstypes.AttributeValue{
		"ID":   &awstypes.AttributeValueMemberS{Value: "itemToDelete"},
		"Data": &awstypes.AttributeValueMemberS{Value: "data to be deleted"},
	}

	_, err = dbClient.PutItem(context.TODO(), &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item:      item,
	})
	if err != nil {
		t.Fatalf("PutItem failed: %v", err)
	}

	_, err = dbClient.DeleteItem(context.TODO(), &dynamodb.DeleteItemInput{
		TableName: aws.String(tableName),
		Key: map[string]awstypes.AttributeValue{
			"ID": &awstypes.AttributeValueMemberS{Value: "itemToDelete"},
		},
	})
	if err != nil {
		t.Fatalf("DeleteItem failed: %v", err)
	}

	getItemOutput, err := dbClient.GetItem(context.TODO(), &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]awstypes.AttributeValue{
			"ID": &awstypes.AttributeValueMemberS{Value: "itemToDelete"},
		},
	})
	if err != nil {
		t.Fatalf("GetItem after DeleteItem failed: %v", err)
	}

	if getItemOutput.Item != nil && len(getItemOutput.Item) > 0 {
		t.Errorf("expected item to be deleted, but it still exists: %v", getItemOutput.Item)
	}
}

func TestQuery(t *testing.T) {
	dbClient, cleanup := setupTestServer(t)
	defer cleanup()

	tableName := "TestQueryTable"
	_, err := dbClient.CreateTable(context.TODO(), &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []awstypes.KeySchemaElement{
			{AttributeName: aws.String("UserID"), KeyType: awstypes.KeyTypeHash},
			{AttributeName: aws.String("Timestamp"), KeyType: awstypes.KeyTypeRange},
		},
		AttributeDefinitions: []awstypes.AttributeDefinition{
			{AttributeName: aws.String("UserID"), AttributeType: awstypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("Timestamp"), AttributeType: awstypes.ScalarAttributeTypeN},
		},
		ProvisionedThroughput: &awstypes.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},
	})
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// Put multiple items for querying
	itemsToPut := []map[string]awstypes.AttributeValue{
		{
			"UserID":    &awstypes.AttributeValueMemberS{Value: "userA"},
			"Timestamp": &awstypes.AttributeValueMemberN{Value: "100"},
			"Data":      &awstypes.AttributeValueMemberS{Value: "dataA1"},
		},
		{
			"UserID":    &awstypes.AttributeValueMemberS{Value: "userA"},
			"Timestamp": &awstypes.AttributeValueMemberN{Value: "200"},
			"Data":      &awstypes.AttributeValueMemberS{Value: "dataA2"},
		},
		{
			"UserID":    &awstypes.AttributeValueMemberS{Value: "userB"},
			"Timestamp": &awstypes.AttributeValueMemberN{Value: "150"},
			"Data":      &awstypes.AttributeValueMemberS{Value: "dataB1"},
		},
	}

	for _, item := range itemsToPut {
		_, err := dbClient.PutItem(context.TODO(), &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item:      item,
		})
		if err != nil {
			t.Fatalf("PutItem failed: %v", err)
		}
	}

	// Query for UserID = "userA"
	queryOutput, err := dbClient.Query(context.TODO(), &dynamodb.QueryInput{
		TableName:              aws.String(tableName),
		KeyConditionExpression: aws.String("UserID = :uid"),
		ExpressionAttributeValues: map[string]awstypes.AttributeValue{
			":uid": &awstypes.AttributeValueMemberS{Value: "userA"},
		},
	})
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(queryOutput.Items) != 2 {
		t.Errorf("expected 2 items for userA, got %d", len(queryOutput.Items))
	}

	// Verify items
	foundTimestamps := make(map[string]bool)
	for _, item := range queryOutput.Items {
		if ts, ok := item["Timestamp"]; ok {
			foundTimestamps[ts.(*awstypes.AttributeValueMemberN).Value] = true
		}
	}

	if !foundTimestamps["100"] || !foundTimestamps["200"] {
		t.Errorf("expected timestamps 100 and 200, got %v", foundTimestamps)
	}
}

func TestScan(t *testing.T) {
	dbClient, cleanup := setupTestServer(t)
	defer cleanup()

	tableName := "TestScanTable"
	_, err := dbClient.CreateTable(context.TODO(), &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []awstypes.KeySchemaElement{
			{AttributeName: aws.String("ID"), KeyType: awstypes.KeyTypeHash},
		},
		AttributeDefinitions: []awstypes.AttributeDefinition{
			{AttributeName: aws.String("ID"), AttributeType: awstypes.ScalarAttributeTypeS},
		},
		ProvisionedThroughput: &awstypes.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},
	})
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// Put multiple items for scanning
	itemsToPut := []map[string]awstypes.AttributeValue{
		{
			"ID":   &awstypes.AttributeValueMemberS{Value: "item1"},
			"Data": &awstypes.AttributeValueMemberS{Value: "data1"},
		},
		{
			"ID":   &awstypes.AttributeValueMemberS{Value: "item2"},
			"Data": &awstypes.AttributeValueMemberS{Value: "data2"},
		},
		{
			"ID":   &awstypes.AttributeValueMemberS{Value: "item3"},
			"Data": &awstypes.AttributeValueMemberS{Value: "data3"},
		},
		{
			"ID":   &awstypes.AttributeValueMemberS{Value: "item4"},
			"Data": &awstypes.AttributeValueMemberS{Value: "data4"},
		},
		{
			"ID":   &awstypes.AttributeValueMemberS{Value: "item5"},
			"Data": &awstypes.AttributeValueMemberS{Value: "data5"},
		},
	}

	for _, item := range itemsToPut {
		_, err := dbClient.PutItem(context.TODO(), &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item:      item,
		})
		if err != nil {
			t.Fatalf("PutItem failed: %v", err)
		}
	}

	// Test full scan (no pagination)
	scanOutput, err := dbClient.Scan(context.TODO(), &dynamodb.ScanInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		t.Fatalf("Full Scan failed: %v", err)
	}

	if len(scanOutput.Items) != len(itemsToPut) {
		t.Errorf("expected %d items for full scan, got %d", len(itemsToPut), len(scanOutput.Items))
	}
	if scanOutput.ScannedCount != int32(len(itemsToPut)) {
		t.Errorf("expected ScannedCount %d, got %d", len(itemsToPut), scanOutput.ScannedCount)
	}
	if scanOutput.LastEvaluatedKey != nil {
		t.Errorf("expected LastEvaluatedKey to be nil for full scan, got %v", scanOutput.LastEvaluatedKey)
	}

	// Test paginated scan
	var allScannedItems []map[string]awstypes.AttributeValue
	var lastEvaluatedKey map[string]awstypes.AttributeValue
	pageSize := int32(2)

	for i := 0; i < (len(itemsToPut)/int(pageSize))+1; i++ {
		input := &dynamodb.ScanInput{
			TableName: aws.String(tableName),
			Limit:     aws.Int32(pageSize),
		}
		if lastEvaluatedKey != nil {
			input.ExclusiveStartKey = lastEvaluatedKey
		}

		pageOutput, err := dbClient.Scan(context.TODO(), input)
		if err != nil {
			t.Fatalf("Paginated Scan failed on page %d: %v", i+1, err)
		}

		allScannedItems = append(allScannedItems, pageOutput.Items...)
		lastEvaluatedKey = pageOutput.LastEvaluatedKey

		if lastEvaluatedKey == nil {
			break // No more pages
		}
	}

	if len(allScannedItems) != len(itemsToPut) {
		t.Errorf("expected %d items after pagination, got %d", len(itemsToPut), len(allScannedItems))
	}

	// Verify all items are present after pagination
	foundItems := make(map[string]bool)
	for _, item := range allScannedItems {
		if id, ok := item["ID"]; ok {
			foundItems[id.(*awstypes.AttributeValueMemberS).Value] = true
		}
	}

	for _, item := range itemsToPut {
		if id, ok := item["ID"]; ok {
			if !foundItems[id.(*awstypes.AttributeValueMemberS).Value] {
				t.Errorf("item with ID %s not found in paginated scan results", id.(*awstypes.AttributeValueMemberS).Value)
			}
		}
	}
}