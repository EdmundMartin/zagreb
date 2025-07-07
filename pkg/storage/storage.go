package storage

import (
	"zagreb/pkg/expression"
	"zagreb/pkg/types"
)

// Storage is an interface for a storage engine.
type Storage interface {
	CreateTable(req *types.CreateTableRequest) error
	Put(req *types.PutRequest) error
	Get(req *types.GetRequest) (map[string]*expression.AttributeValue, error)
	Delete(req *types.DeleteRequest) error
	Update(req *types.UpdateRequest) (map[string]*expression.AttributeValue, error)
	Query(req *types.QueryRequest) ([]map[string]*expression.AttributeValue, error)
}
