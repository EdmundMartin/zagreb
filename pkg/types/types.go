package types

import (
	"zagreb/pkg/expression"

	
)

// AttributeValue represents a DynamoDB attribute value.
type AttributeValue = expression.AttributeValue

// KeySchemaElement defines the schema for a key.
type KeySchemaElement struct {
	AttributeName string `json:"AttributeName"`
	KeyType       string `json:"KeyType"`
}

// AttributeDefinition defines an attribute for a table.
type AttributeDefinition struct {
	AttributeName string `json:"AttributeName"`
	AttributeType string `json:"AttributeType"`
}

// CreateTableRequest represents a DynamoDB CreateTable request.
type CreateTableRequest struct {
	TableName            string                 `json:"TableName"`
	KeySchema            []*KeySchemaElement    `json:"KeySchema"`
	AttributeDefinitions []*AttributeDefinition `json:"AttributeDefinitions"`
}

// PutRequest represents a DynamoDB PutItem request.
type PutRequest struct {
	TableName string                     `json:"TableName"`
	Item      map[string]*AttributeValue `json:"Item"`
}

// GetRequest represents a DynamoDB GetItem request.
type GetRequest struct {
	TableName string                     `json:"TableName"`
	Key       map[string]*AttributeValue `json:"Key"`
}

// DeleteRequest represents a DynamoDB DeleteItem request.
type DeleteRequest struct {
	TableName string                     `json:"TableName"`
	Key       map[string]*AttributeValue `json:"Key"`
}

// UpdateRequest represents a DynamoDB UpdateItem request.
type UpdateRequest struct {
	TableName                 string                     `json:"TableName"`
	Key                       map[string]*AttributeValue `json:"Key"`
	UpdateExpression          string                     `json:"UpdateExpression"`
	ExpressionAttributeValues map[string]*AttributeValue `json:"ExpressionAttributeValues,omitempty"`
}

// QueryRequest represents a DynamoDB Query request.
type QueryRequest struct {
	TableName                 string                     `json:"TableName"`
	KeyConditionExpression    string                     `json:"KeyConditionExpression"`
	ExpressionAttributeValues map[string]*AttributeValue `json:"ExpressionAttributeValues,omitempty"`
}
