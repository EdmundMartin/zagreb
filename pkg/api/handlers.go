package api

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"zagreb/pkg/expression"
	"zagreb/pkg/types"
)

// writeJSONError writes a JSON error response to the http.ResponseWriter.
func writeJSONError(w http.ResponseWriter, err error, statusCode int) {
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"__type":    "com.amazonaws.dynamodb.v20120810#" + http.StatusText(statusCode), // Simplified error type
		"message":   err.Error(),
	})
}

// handleRequest is a generic handler for all DynamoDB-like operations.
func (s *Server) handleRequest(w http.ResponseWriter, r *http.Request) {
	// Read the request body
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		writeJSONError(w, fmt.Errorf("Error reading request body: %v", err), http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()

	// Parse the target header to determine the operation
	target := r.Header.Get("X-Amz-Target")
	if target == "" {
		writeJSONError(w, fmt.Errorf("X-Amz-Target header missing"), http.StatusBadRequest)
		return
	}

	// DynamoDB uses a specific format for the target header, e.g., "DynamoDB_20120810.CreateTable"
	switch target {
	case "DynamoDB_20120810.CreateTable":
		s.handleCreateTable(w, body)
	case "DynamoDB_20120810.PutItem":
		s.handlePutItem(w, body)
	case "DynamoDB_20120810.GetItem":
		s.handleGetItem(w, body)
	case "DynamoDB_20120810.UpdateItem":
		s.handleUpdateItem(w, body)
	case "DynamoDB_20120810.DeleteItem":
		s.handleDeleteItem(w, body)
	case "DynamoDB_20120810.Query":
		s.handleQuery(w, body)
	default:
		writeJSONError(w, fmt.Errorf("Unsupported operation: %s", target), http.StatusBadRequest)
	}
}

func (s *Server) handleCreateTable(w http.ResponseWriter, body []byte) {
	var req types.CreateTableRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeJSONError(w, fmt.Errorf("Invalid CreateTable request format: %v", err), http.StatusBadRequest)
		return
	}

	if err := s.storage.CreateTable(&req); err != nil {
		writeJSONError(w, fmt.Errorf("Error creating table: %v", err), http.StatusInternalServerError)
		return
	}

	// Mimic DynamoDB's CreateTable response
	resp := map[string]interface{}{
		"TableDescription": map[string]interface{}{
			"TableName":            req.TableName,
			"TableStatus":          "ACTIVE", // Simplified status
			"CreationDateTime":     time.Now().Unix(),
			"ProvisionedThroughput": map[string]int64{
				"NumberOfDecreasesToday": 0,
				"ReadCapacityUnits":      1, // Dummy values
				"WriteCapacityUnits":     1, // Dummy values
			},
			"KeySchema":            req.KeySchema,
			"AttributeDefinitions": req.AttributeDefinitions,
			"TableSizeBytes":       0,
			"ItemCount":            0,
		},
	}
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) handlePutItem(w http.ResponseWriter, body []byte) {
	var req types.PutRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeJSONError(w, fmt.Errorf("Invalid PutItem request format: %v", err), http.StatusBadRequest)
		return
	}

	if err := s.storage.Put(&req); err != nil {
		writeJSONError(w, fmt.Errorf("Error putting item: %v", err), http.StatusInternalServerError)
		return
	}

	// DynamoDB PutItem response is typically empty unless ReturnValues is specified
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	json.NewEncoder(w).Encode(map[string]interface{}{})
}

func (s *Server) handleGetItem(w http.ResponseWriter, body []byte) {
	var req types.GetRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeJSONError(w, fmt.Errorf("Invalid GetItem request format: %v", err), http.StatusBadRequest)
		return
	}

	item, err := s.storage.Get(&req)
	if err != nil {
		writeJSONError(w, fmt.Errorf("Error getting item: %v", err), http.StatusInternalServerError)
		return
	}

	if item == nil {
		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		json.NewEncoder(w).Encode(map[string]interface{}{}) // Return empty map if item not found
		return
	}

	responseMap := map[string]interface{}{
		"Item": convertAttributeValueMap(item),
	}

	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	json.NewEncoder(w).Encode(responseMap)
}

func (s *Server) handleUpdateItem(w http.ResponseWriter, body []byte) {
	var req types.UpdateRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeJSONError(w, fmt.Errorf("Invalid UpdateItem request format: %v", err), http.StatusBadRequest)
		return
	}

	updatedItem, err := s.storage.Update(&req)
	if err != nil {
		writeJSONError(w, fmt.Errorf("Error updating item: %v", err), http.StatusInternalServerError)
		return
	}

	// DynamoDB UpdateItem response returns Attributes if ReturnValues is specified
	responseMap := map[string]interface{}{
		"Attributes": convertAttributeValueMap(updatedItem),
	}

	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	json.NewEncoder(w).Encode(responseMap)
}

func (s *Server) handleDeleteItem(w http.ResponseWriter, body []byte) {
	var req types.DeleteRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeJSONError(w, fmt.Errorf("Invalid DeleteItem request format: %v", err), http.StatusBadRequest)
		return
	}

	if err := s.storage.Delete(&req); err != nil {
		writeJSONError(w, fmt.Errorf("Error deleting item: %v", err), http.StatusInternalServerError)
		return
	}

	// DynamoDB DeleteItem response is typically empty unless ReturnValues is specified
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	json.NewEncoder(w).Encode(map[string]interface{}{})
}

func (s *Server) handleQuery(w http.ResponseWriter, body []byte) {
	var req types.QueryRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeJSONError(w, fmt.Errorf("Invalid Query request format: %v", err), http.StatusBadRequest)
		return
	}

	items, err := s.storage.Query(&req)
	if err != nil {
		writeJSONError(w, fmt.Errorf("Error querying items: %v", err), http.StatusInternalServerError)
		return
	}

	responseItems := make([]map[string]interface{}, len(items))
	for i, item := range items {
		responseItems[i] = convertAttributeValueMap(item)
	}

	responseMap := map[string]interface{}{
		"Items": responseItems,
		"Count": len(responseItems),
	}

	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	json.NewEncoder(w).Encode(responseMap)
}

// convertAttributeValueMap converts a map of our internal expression.AttributeValues
// to a map suitable for marshaling into the DynamoDB JSON format.
func convertAttributeValueMap(internalMap map[string]*expression.AttributeValue) map[string]interface{} {
	if internalMap == nil {
		return nil
	}
	responseMap := make(map[string]interface{}, len(internalMap))
	for k, v := range internalMap {
		responseMap[k] = convertAttributeValue(v)
	}
	return responseMap
}

// convertAttributeValue converts a single internal expression.AttributeValue
// to a map suitable for marshaling into the DynamoDB JSON format.
func convertAttributeValue(attrVal *expression.AttributeValue) map[string]interface{} {
	if attrVal == nil {
		return nil // Or handle as NULL
	}
	if attrVal.S != nil {
		return map[string]interface{}{"S": *attrVal.S}
	}
	if attrVal.N != nil {
		return map[string]interface{}{"N": *attrVal.N}
	}
	if attrVal.B != nil {
		return map[string]interface{}{"B": attrVal.B}
	}
	if attrVal.SS != nil {
		return map[string]interface{}{"SS": attrVal.SS}
	}
	if attrVal.NS != nil {
		return map[string]interface{}{"NS": attrVal.NS}
	}
	if attrVal.BS != nil {
		return map[string]interface{}{"BS": attrVal.BS}
	}
	if attrVal.M != nil {
		subMap := make(map[string]interface{}, len(attrVal.M))
		for k, v := range attrVal.M {
			subMap[k] = convertAttributeValue(v)
		}
		return map[string]interface{}{"M": subMap}
	}
	if attrVal.L != nil {
		subList := make([]interface{}, len(attrVal.L))
		for i, v := range attrVal.L {
			subList[i] = convertAttributeValue(v)
		}
		return map[string]interface{}{"L": subList}
	}
	if attrVal.NULL != nil && *attrVal.NULL {
		return map[string]interface{}{"NULL": true}
	}
	if attrVal.BOOL != nil {
		return map[string]interface{}{"BOOL": *attrVal.BOOL}
	}
	// Default case, should ideally not be reached with valid data
	return map[string]interface{}{}
}