package bbolt

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	bolt "go.etcd.io/bbolt"
	"zagreb/pkg/expression"
	"zagreb/pkg/types"
)

const (
	metadataBucket = "_metadata"
	keyDelimiter   = "|"
)

// BBoltStorage is a storage engine that uses bbolt.
type BBoltStorage struct {
	db *bolt.DB
}

// NewBBoltStorage creates a new BBoltStorage.
func NewBBoltStorage(path string) (*BBoltStorage, error) {
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		return nil, err
	}

	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(metadataBucket))
		return err
	})

	if err != nil {
		return nil, err
	}

	return &BBoltStorage{db: db}, nil
}

// CreateTable creates a new table.
func (s *BBoltStorage) CreateTable(req *types.CreateTableRequest) (*types.CreateTableResponse, error) {
	err := s.db.Update(func(tx *bolt.Tx) error {
		// Create the table bucket.
		_, err := tx.CreateBucketIfNotExists([]byte(req.TableName))
		if err != nil {
			return err
		}

		// Store the table definition.
		mb := tx.Bucket([]byte(metadataBucket))
		key := []byte(req.TableName)
		val, err := json.Marshal(req)
		if err != nil {
			return err
		}

		return mb.Put(key, val)
	})

	if err != nil {
		return nil, err
	}

	return &types.CreateTableResponse{
		TableDescription: types.TableDescription{
			TableName:            req.TableName,
			KeySchema:            req.KeySchema,
			AttributeDefinitions: req.AttributeDefinitions,
		},
	}, nil
}

// DeleteTable deletes a table.
func (s *BBoltStorage) DeleteTable(req *types.DeleteTableRequest) (*types.DeleteTableResponse, error) {
	var tableDef *types.CreateTableRequest

	err := s.db.Update(func(tx *bolt.Tx) error {
		// Get table definition
		var err error
		tableDef, err = s.getTableDef(tx, req.TableName)
		if err != nil {
			return err
		}

		// Delete the table bucket.
		if err := tx.DeleteBucket([]byte(req.TableName)); err != nil {
			return err
		}

		// Delete the table definition.
		mb := tx.Bucket([]byte(metadataBucket))
		return mb.Delete([]byte(req.TableName))
	})

	if err != nil {
		return nil, err
	}

	return &types.DeleteTableResponse{
		TableDescription: types.TableDescription{
			TableName:            tableDef.TableName,
			KeySchema:            tableDef.KeySchema,
			AttributeDefinitions: tableDef.AttributeDefinitions,
		},
	}, nil
}

// DescribeTable describes a table.
func (s *BBoltStorage) DescribeTable(req *types.DescribeTableRequest) (*types.DescribeTableResponse, error) {
	var tableDef *types.CreateTableRequest

	err := s.db.View(func(tx *bolt.Tx) error {
		var err error
		tableDef, err = s.getTableDef(tx, req.TableName)
		return err
	})

	if err != nil {
		return nil, err
	}

	return &types.DescribeTableResponse{
		Table: types.TableDescription{
			TableName:            tableDef.TableName,
			KeySchema:            tableDef.KeySchema,
			AttributeDefinitions: tableDef.AttributeDefinitions,
		},
	}, nil
}

// ListTables lists all tables.
func (s *BBoltStorage) ListTables(req *types.ListTablesRequest) (*types.ListTablesResponse, error) {
	var tableNames []string

	err := s.db.View(func(tx *bolt.Tx) error {
		mb := tx.Bucket([]byte(metadataBucket))
		return mb.ForEach(func(k, v []byte) error {
			tableNames = append(tableNames, string(k))
			return nil
		})
	})

	if err != nil {
		return nil, err
	}

	return &types.ListTablesResponse{TableNames: tableNames}, nil
}

// Put adds an item to a table.
func (s *BBoltStorage) Put(req *types.PutRequest) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		tableDef, err := s.getTableDef(tx, req.TableName)
		if err != nil {
			return err
		}

		if err := s.validatePutRequest(tableDef, req); err != nil {
			return err
		}

		// Get the bucket for the table.
		b := tx.Bucket([]byte(req.TableName))
		if b == nil {
			return fmt.Errorf("bucket not found: %s", req.TableName)
		}

		// Generate the key string for the item.
		keyStr, err := s.generateKeyString(tableDef, req.Item)
		if err != nil {
			return err
		}
		key := []byte(keyStr)

		// Marshal the item to JSON.
		val, err := json.Marshal(req.Item)
		if err != nil {
			return err
		}

		return b.Put(key, val)
	})
}

// Get retrieves an item from a table.
func (s *BBoltStorage) Get(req *types.GetRequest) (map[string]*expression.AttributeValue, error) {
	var item map[string]*expression.AttributeValue

	err := s.db.View(func(tx *bolt.Tx) error {
		tableDef, err := s.getTableDef(tx, req.TableName)
		if err != nil {
			return err
		}

		if err := s.validateGetRequest(tableDef, req); err != nil {
			return err
		}

		// Get the bucket for the table.
		b := tx.Bucket([]byte(req.TableName))
		if b == nil {
			return fmt.Errorf("bucket not found: %s", req.TableName)
		}

		// Generate the key string for the item.
		keyStr, err := s.generateKeyString(tableDef, req.Key)
		if err != nil {
			return err
		}
		key := []byte(keyStr)

		val := b.Get(key)
		if val == nil {
			return nil // not found
		}

		return json.Unmarshal(val, &item)
	})

	if err != nil {
		return nil, err
	}

	return item, nil
}

// Delete removes an item from a table.
func (s *BBoltStorage) Delete(req *types.DeleteRequest) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		tableDef, err := s.getTableDef(tx, req.TableName)
		if err != nil {
			return err
		}

		if err := s.validateDeleteRequest(tableDef, req); err != nil {
			return err
		}

		// Get the bucket for the table.
		b := tx.Bucket([]byte(req.TableName))
		if b == nil {
			return fmt.Errorf("bucket not found: %s", req.TableName)
		}

		// Generate the key string for the item.
		keyStr, err := s.generateKeyString(tableDef, req.Key)
		if err != nil {
			return err
		}
		key := []byte(keyStr)

		return b.Delete(key)
	})
}

// Update updates an item in a table.
func (s *BBoltStorage) Update(req *types.UpdateRequest) (map[string]*expression.AttributeValue, error) {
	var updatedItem map[string]*expression.AttributeValue

	err := s.db.Update(func(tx *bolt.Tx) error {
		tableDef, err := s.getTableDef(tx, req.TableName)
		if err != nil {
			return err
		}

		if err := s.validateUpdateRequest(tableDef, req); err != nil {
			return err
		}

		// Get the bucket for the table.
		b := tx.Bucket([]byte(req.TableName))
		if b == nil {
			return fmt.Errorf("bucket not found: %s", req.TableName)
		}

		// Generate the key string for the item.
		keyStr, err := s.generateKeyString(tableDef, req.Key)
		if err != nil {
			return err
		}
		key := []byte(keyStr)

		val := b.Get(key)
		if val == nil {
			return fmt.Errorf("item not found")
		}

		var item map[string]*expression.AttributeValue
		if err := json.Unmarshal(val, &item); err != nil {
			return err
		}

				updatedItem, err = expression.Update(item, req.UpdateExpression, req.ExpressionAttributeValues)
		if err != nil {
			return err
		}

		newVal, err := json.Marshal(updatedItem)
		if err != nil {
			return err
		}

		return b.Put(key, newVal)
	})

	if err != nil {
		return nil, err
	}

	return updatedItem, nil
}

// Query queries a table.
func (s *BBoltStorage) Query(req *types.QueryRequest) ([]map[string]*expression.AttributeValue, error) {
	var items []map[string]*expression.AttributeValue

	err := s.db.View(func(tx *bolt.Tx) error {
		tableDef, err := s.getTableDef(tx, req.TableName)
		if err != nil {
			return err
		}

		if err := s.validateQueryRequest(tableDef, req); err != nil {
			return err
		}

		// Get the bucket for the table.
		b := tx.Bucket([]byte(req.TableName))
		if b == nil {
			return fmt.Errorf("bucket not found: %s", req.TableName)
		}

		// This is a simplified implementation of Query that only supports querying by hash key.
		parts := strings.Split(req.KeyConditionExpression, " ")
		if len(parts) != 3 || parts[1] != "=" {
			return fmt.Errorf("invalid key condition expression format")
		}
		hashKeyName := parts[0]
		hashKeyValuePlaceholder := parts[2]

		// Look up the actual value from ExpressionAttributeValues
		hashKeyValue, ok := req.ExpressionAttributeValues[hashKeyValuePlaceholder]
		if !ok {
			return fmt.Errorf("expression attribute value not found: %s", hashKeyValuePlaceholder)
		}

		// Construct the seek key string for bbolt based on the hash key.
		// This must match the prefix generated by generateKeyString.
		seekKeyMap := map[string]*expression.AttributeValue{
			hashKeyName: hashKeyValue,
		}
		seekKeyStr, err := s.generateKeyString(tableDef, seekKeyMap)
		if err != nil {
			return fmt.Errorf("failed to generate seek key string: %v", err)
		}
		seekKey := []byte(seekKeyStr)

		c := b.Cursor()

		// Seek to the first key that matches the hash key prefix.
		for k, v := c.Seek(seekKey); k != nil && bytes.HasPrefix(k, seekKey); k, v = c.Next() {
			var item map[string]*expression.AttributeValue
			if err := json.Unmarshal(v, &item); err != nil {
				return err
			}

			// Double-check the hash key match (redundant if seek/prefix logic is perfect, but safe).
			if item[hashKeyName] != nil && s.compareAttributeValues(item[hashKeyName], hashKeyValue) {
				items = append(items, item)
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return items, nil
}

// generateKeyString creates a deterministic string key for bbolt.
// It concatenates the hash key and range key (if present) values.
func (s *BBoltStorage) generateKeyString(tableDef *types.CreateTableRequest, item map[string]*expression.AttributeValue) (string, error) {
	var hashKeyVal string
	var rangeKeyVal string

	for _, ks := range tableDef.KeySchema {
		attrVal, ok := item[ks.AttributeName]
		if !ok {
			if ks.KeyType == "HASH" {
				return "", fmt.Errorf("missing key attribute %s in item", ks.AttributeName)
			}
			// It's okay for a range key to be missing in a query
			continue
		}

		// Convert AttributeValue to string for key concatenation.
		var valStr string
		switch expression.GetAttributeValueType(attrVal) {
		case "S":
			valStr = *attrVal.S
		case "N":
			valStr = *attrVal.N
		case "BOOL":
			valStr = strconv.FormatBool(*attrVal.BOOL)
		case "NULL":
			valStr = "NULL"
		default:
			return "", fmt.Errorf("unsupported attribute type for key: %s", expression.GetAttributeValueType(attrVal))
		}

		if ks.KeyType == "HASH" {
			hashKeyVal = valStr
		} else if ks.KeyType == "RANGE" {
			rangeKeyVal = valStr
		}
	}

	if hashKeyVal == "" {
		return "", fmt.Errorf("hash key not found in item")
	}

	// Construct the key string.
	key := hashKeyVal
	if rangeKeyVal != "" {
		key += keyDelimiter + rangeKeyVal
	}

	return key, nil
}

func (s *BBoltStorage) compareAttributeValues(val1, val2 *expression.AttributeValue) bool {
	if expression.GetAttributeValueType(val1) != expression.GetAttributeValueType(val2) {
		return false
	}

	switch expression.GetAttributeValueType(val1) {
	case "S":
		return *val1.S == *val2.S
	case "N":
		return *val1.N == *val2.N
	case "BOOL":
		return *val1.BOOL == *val2.BOOL
	case "NULL":
		return true
	// Add other types as needed
	default:
		return false
	}
}

func (s *BBoltStorage) getTableDef(tx *bolt.Tx, tableName string) (*types.CreateTableRequest, error) {
	mb := tx.Bucket([]byte(metadataBucket))
	val := mb.Get([]byte(tableName))
	if val == nil {
		return nil, fmt.Errorf("table not found: %s", tableName)
	}

	var tableDef types.CreateTableRequest
	if err := json.Unmarshal(val, &tableDef); err != nil {
		return nil, err
	}
	return &tableDef, nil
}

func (s *BBoltStorage) validatePutRequest(tableDef *types.CreateTableRequest, req *types.PutRequest) error {
	for _, ks := range tableDef.KeySchema {
		if _, ok := req.Item[ks.AttributeName]; !ok {
			return fmt.Errorf("missing key attribute: %s", ks.AttributeName)
		}
	}

	return nil
}

func (s *BBoltStorage) validateGetRequest(tableDef *types.CreateTableRequest, req *types.GetRequest) error {
	keySchema := make(map[string]string)
	for _, ks := range tableDef.KeySchema {
		keySchema[ks.AttributeName] = ks.KeyType
	}

	if len(req.Key) != len(keySchema) {
		return fmt.Errorf("invalid number of key attributes: expected %d, got %d", len(keySchema), len(req.Key))
	}

	for name := range req.Key {
		if _, ok := keySchema[name]; !ok {
			return fmt.Errorf("invalid key attribute: %s", name)
		}
	}

	return nil
}

func (s *BBoltStorage) validateDeleteRequest(tableDef *types.CreateTableRequest, req *types.DeleteRequest) error {
	keySchema := make(map[string]string)
	for _, ks := range tableDef.KeySchema {
		keySchema[ks.AttributeName] = ks.KeyType
	}

	if len(req.Key) != len(keySchema) {
		return fmt.Errorf("invalid number of key attributes: expected %d, got %d", len(keySchema), len(req.Key))
	}

	for name := range req.Key {
		if _, ok := keySchema[name]; !ok {
			return fmt.Errorf("invalid key attribute: %s", name)
		}
	}

	return nil
}

func (s *BBoltStorage) validateUpdateRequest(tableDef *types.CreateTableRequest, req *types.UpdateRequest) error {
	keySchema := make(map[string]string)
	for _, ks := range tableDef.KeySchema {
		keySchema[ks.AttributeName] = ks.KeyType
	}

	if len(req.Key) != len(keySchema) {
		return fmt.Errorf("invalid number of key attributes: expected %d, got %d", len(keySchema), len(req.Key))
	}

	for name := range req.Key {
		if _, ok := keySchema[name]; !ok {
			return fmt.Errorf("invalid key attribute: %s", name)
		}
	}

	return nil
}

func (s *BBoltStorage) validateQueryRequest(tableDef *types.CreateTableRequest, req *types.QueryRequest) error {
	parts := strings.Split(req.KeyConditionExpression, " ")
	if len(parts) != 3 || parts[1] != "=" {
		return fmt.Errorf("invalid KeyConditionExpression format: expected 'attributeName = value'")
	}

	attrName := parts[0]
	hashKeyValuePlaceholder := parts[2]

	// Find the hash key from the table definition
	hashKeyDef := types.AttributeDefinition{}
	hashKeyFound := false
	for _, ks := range tableDef.KeySchema {
		if ks.KeyType == "HASH" {
			for _, ad := range tableDef.AttributeDefinitions {
				if ad.AttributeName == ks.AttributeName {
					hashKeyDef = *ad
					hashKeyFound = true
					break
				}
			}
			break
		}
	}

	if !hashKeyFound {
		return fmt.Errorf("hash key not found in table definition")
	}

	// Validate that the attribute name in the expression matches the hash key name
	if attrName != hashKeyDef.AttributeName {
		return fmt.Errorf("KeyConditionExpression must use the hash key '%s', but got '%s'", hashKeyDef.AttributeName, attrName)
	}

	// Validate the type of the value in the expression
	attrVal, ok := req.ExpressionAttributeValues[hashKeyValuePlaceholder]
	if !ok {
		return fmt.Errorf("expression attribute value not found: %s", hashKeyValuePlaceholder)
	}

	if expression.GetAttributeValueType(attrVal) != hashKeyDef.AttributeType {
		return fmt.Errorf("invalid type for hash key '%s': expected %s, got %s", hashKeyDef.AttributeName, hashKeyDef.AttributeType, expression.GetAttributeValueType(attrVal))
	}

	return nil
}

