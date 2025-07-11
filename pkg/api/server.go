package api

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/gorilla/mux"
	"zagreb/pkg/expression"
	"zagreb/pkg/router"
	"zagreb/pkg/routerapi"
	"zagreb/pkg/storage"
	"zagreb/pkg/types"
)

// Server represents the HTTP API server.
type Server struct {
	storage storage.Storage
	router  *mux.Router
	routerInstance *router.Router // Added to access router methods for node management
}

// NewServer creates a new Server instance.
func NewServer(s storage.Storage) *Server {
	server := &Server{
		storage: s,
		router:  mux.NewRouter(),
	}
	server.routes()
	return server
}

// NewRouterServer creates a new Server instance specifically for the router.
func NewRouterServer(r *router.Router) *Server {
	server := &Server{
		storage: r, // The router itself implements the Storage interface
		router:  mux.NewRouter(),
		routerInstance: r,
	}
	server.routes()
	server.router.HandleFunc("/register-node", server.handleRegisterNode).Methods("POST")
	server.router.HandleFunc("/deregister-node", server.handleDeregisterNode).Methods("POST")
	return server
}

// Router returns the mux.Router instance.
func (s *Server) Router() *mux.Router {
	return s.router
}

// Run starts the HTTP server.
func (s *Server) Run(addr string) {
	log.Printf("Server listening on %s\n", addr)
	log.Fatal(http.ListenAndServe(addr, s.router))
}

func (s *Server) routes() {
	// DynamoDB-like API endpoints
	s.router.HandleFunc("/", s.handleRequest).Methods("POST")

	// Internal API for node-to-node communication
	s.router.HandleFunc("/internal-scan", s.handleInternalScan).Methods("POST")
}

// handleRequest is a generic handler for all DynamoDB-like operations.
func (s *Server) handleRequest(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	target, ok := r.Header["X-Amz-Target"]
	if !ok || len(target) == 0 {
		s.writeError(w, "missing X-Amz-Target header", http.StatusBadRequest)
		return
	}
	action := strings.Split(target[0], ".")[1]

	var body json.RawMessage
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		s.writeError(w, "failed to decode request body", http.StatusBadRequest)
		return
	}

	switch action {
	case "CreateTable":
		var req types.CreateTableRequest
		if err := json.Unmarshal(body, &req); err != nil {
			s.writeError(w, err.Error(), http.StatusBadRequest)
			return
		}
		resp, err := s.storage.CreateTable(&req)
		if err != nil {
			s.writeError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp)
	case "DeleteTable":
		var req types.DeleteTableRequest
		if err := json.Unmarshal(body, &req); err != nil {
			s.writeError(w, err.Error(), http.StatusBadRequest)
			return
		}
		resp, err := s.storage.DeleteTable(&req)
		if err != nil {
			s.writeError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp)
	case "DescribeTable":
		var req types.DescribeTableRequest
		if err := json.Unmarshal(body, &req); err != nil {
			s.writeError(w, err.Error(), http.StatusBadRequest)
			return
		}
		resp, err := s.storage.DescribeTable(&req)
		if err != nil {
			s.writeError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp)
	case "ListTables":
		var req types.ListTablesRequest
		if err := json.Unmarshal(body, &req); err != nil {
			s.writeError(w, err.Error(), http.StatusBadRequest)
			return
		}
		resp, err := s.storage.ListTables(&req)
		if err != nil {
			s.writeError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp)
	case "PutItem":
		var putReq types.PutRequest
		if err := json.Unmarshal(body, &putReq); err != nil {
			s.writeError(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := s.storage.Put(&putReq); err != nil {
			s.writeError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{}) // Empty object for success
	case "GetItem":
		var getReq types.GetRequest
		if err := json.Unmarshal(body, &getReq); err != nil {
			s.writeError(w, err.Error(), http.StatusBadRequest)
			return
		}
		item, err := s.storage.Get(&getReq)
		if err != nil {
			s.writeError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(types.GetItemResponse{Item: item})
	case "DeleteItem":
		var deleteReq types.DeleteRequest
		if err := json.Unmarshal(body, &deleteReq); err != nil {
			s.writeError(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := s.storage.Delete(&deleteReq); err != nil {
			s.writeError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{}) // Empty object for success
	case "UpdateItem":
		var updateReq types.UpdateRequest
		if err := json.Unmarshal(body, &updateReq); err != nil {
			s.writeError(w, err.Error(), http.StatusBadRequest)
			return
		}
		item, err := s.storage.Update(&updateReq)
		if err != nil {
			s.writeError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(types.UpdateItemResponse{Attributes: item})
	case "Query":
		var queryReq types.QueryRequest
		if err := json.Unmarshal(body, &queryReq); err != nil {
			s.writeError(w, err.Error(), http.StatusBadRequest)
			return
		}
		items, err := s.storage.Query(&queryReq)
		if err != nil {
			s.writeError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(types.QueryResponse{Items: items})
	case "Scan":
		var rawScanReq struct {
			TableName         string                     `json:"TableName"`
			Limit             *int                       `json:"Limit,omitempty"`
			ExclusiveStartKey map[string]interface{} `json:"ExclusiveStartKey,omitempty"`
		}
		if err := json.Unmarshal(body, &rawScanReq); err != nil {
			s.writeError(w, err.Error(), http.StatusBadRequest)
			return
		}

		scanReq := types.ScanRequest{
			TableName: rawScanReq.TableName,
			Limit:     rawScanReq.Limit,
		}

		if rawScanReq.ExclusiveStartKey != nil {
			exclusiveStartKey, err := convertAWSToExpressionAttributeValue(rawScanReq.ExclusiveStartKey)
			if err != nil {
				s.writeError(w, err.Error(), http.StatusBadRequest)
				return
			}
			scanReq.ExclusiveStartKey = exclusiveStartKey
		}

		resp, err := s.storage.Scan(&scanReq)
		if err != nil {
			s.writeError(w, err.Error(), http.StatusInternalServerError)
			return
		}

		awsScanResp := struct {
			Items            []map[string]*expression.AttributeValue `json:"Items"`
			LastEvaluatedKey map[string]interface{}            `json:"LastEvaluatedKey,omitempty"`
			ScannedCount     int                               `json:"ScannedCount"`
		}{
			Items:        resp.Items,
			ScannedCount: resp.ScannedCount,
		}

		if resp.LastEvaluatedKey != nil {
			convertedLastEvaluatedKey, err := convertExpressionToAWSAttributeValue(resp.LastEvaluatedKey)
			if err != nil {
				s.writeError(w, err.Error(), http.StatusInternalServerError)
				return
			}
			awsScanResp.LastEvaluatedKey = convertedLastEvaluatedKey
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(awsScanResp)
	default:
		s.writeError(w, "unknown action: "+action, http.StatusBadRequest)
	}
}

func (s *Server) writeError(w http.ResponseWriter, message string, statusCode int) {
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(map[string]string{"message": message})
}

func (s *Server) handleRegisterNode(w http.ResponseWriter, r *http.Request) {
	if s.routerInstance == nil {
		http.Error(w, "router instance not set", http.StatusInternalServerError)
		return
	}

	var req routerapi.RegisterNodeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	s.routerInstance.AddNode(router.Node{ID: req.ID, Addr: req.Addr})

	resp := routerapi.RegisterNodeResponse{
		ActiveNodes: s.routerInstance.GetActiveNodes(),
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleDeregisterNode(w http.ResponseWriter, r *http.Request) {
	if s.routerInstance == nil {
		http.Error(w, "router instance not set", http.StatusInternalServerError)
		return
	}

	var req routerapi.DeregisterNodeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	s.routerInstance.RemoveNode(req.ID)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleInternalScan(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var rawScanReq struct {
		TableName         string                     `json:"TableName"`
		Limit             *int                       `json:"Limit,omitempty"`
		ExclusiveStartKey map[string]interface{} `json:"ExclusiveStartKey,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&rawScanReq); err != nil {
		s.writeError(w, "failed to decode request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	scanReq := types.ScanRequest{
		TableName: rawScanReq.TableName,
		Limit:     rawScanReq.Limit,
	}

	if rawScanReq.ExclusiveStartKey != nil {
		exclusiveStartKey, err := convertAWSToExpressionAttributeValue(rawScanReq.ExclusiveStartKey)
		if err != nil {
			s.writeError(w, err.Error(), http.StatusBadRequest)
			return
		}
		scanReq.ExclusiveStartKey = exclusiveStartKey
	}

	resp, err := s.storage.InternalScan(&scanReq)
	if err != nil {
		s.writeError(w, "failed to perform internal scan: "+err.Error(), http.StatusInternalServerError)
		return
	}

	awsScanResp := struct {
		Items            []map[string]*expression.AttributeValue `json:"Items"`
		LastEvaluatedKey map[string]interface{}            `json:"LastEvaluatedKey,omitempty"`
		ScannedCount     int                               `json:"ScannedCount"`
	}{
		Items:        resp.Items,
		ScannedCount: resp.ScannedCount,
	}

	if resp.LastEvaluatedKey != nil {
		convertedLastEvaluatedKey, err := convertExpressionToAWSAttributeValue(resp.LastEvaluatedKey)
		if err != nil {
			s.writeError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		awsScanResp.LastEvaluatedKey = convertedLastEvaluatedKey
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(awsScanResp)
}

// convertAWSToExpressionAttributeValue converts a map of AWS SDK AttributeValue (represented as map[string]interface{})
// to our internal expression.AttributeValue.
// convertAWSToExpressionAttributeValue converts a map of AWS SDK AttributeValue (represented as map[string]interface{})
// to our internal expression.AttributeValue.
func convertAWSToExpressionAttributeValue(awsMap map[string]interface{}) (map[string]*expression.AttributeValue, error) {
	expMap := make(map[string]*expression.AttributeValue)
	for k, v := range awsMap {
		// Each value 'v' is expected to be a map with a single key representing the type (e.g., "S", "N")
		// and its corresponding value.
		attrMap, ok := v.(map[string]interface{})
		if !ok || len(attrMap) != 1 {
			return nil, fmt.Errorf("invalid AWS attribute value format for key %s: expected a map with single type key", k)
		}

		for typeKey, typeVal := range attrMap {
			var exprAttrVal expression.AttributeValue
			switch typeKey {
			case "S":
				strVal, ok := typeVal.(string)
				if !ok {
					return nil, fmt.Errorf("invalid type for S attribute for key %s: expected string", k)
				}
				exprAttrVal.S = &strVal
			case "N":
				// Numbers are often unmarshaled as string in AWS SDK for DynamoDB
				strVal, ok := typeVal.(string)
				if !ok {
					return nil, fmt.Errorf("invalid type for N attribute for key %s: expected string", k)
				}
				exprAttrVal.N = &strVal
			case "BOOL":
				boolVal, ok := typeVal.(bool)
				if !ok {
					return nil, fmt.Errorf("invalid type for BOOL attribute for key %s: expected bool", k)
				}
				exprAttrVal.BOOL = &boolVal
			case "NULL":
				nullVal, ok := typeVal.(bool)
				if !ok {
					return nil, fmt.Errorf("invalid type for NULL attribute for key %s: expected bool", k)
				}
				exprAttrVal.NULL = &nullVal
			default:
				return nil, fmt.Errorf("unsupported AWS attribute type '%s' for key %s", typeKey, k)
			}
			expMap[k] = &exprAttrVal
		}
	}
	return expMap, nil
}

// convertExpressionToAWSAttributeValue converts our internal expression.AttributeValue to a map suitable for AWS SDK JSON marshalling.
func convertExpressionToAWSAttributeValue(expMap map[string]*expression.AttributeValue) (map[string]interface{}, error) {
	awsMap := make(map[string]interface{})
	for k, v := range expMap {
		if v == nil {
			continue
		}
		// Determine the type and create the corresponding AWS SDK-like structure
		if v.S != nil {
			awsMap[k] = map[string]interface{}{"S": *v.S}
		} else if v.N != nil {
			awsMap[k] = map[string]interface{}{"N": *v.N}
		} else if v.BOOL != nil {
			awsMap[k] = map[string]interface{}{"BOOL": *v.BOOL}
		} else if v.NULL != nil {
			awsMap[k] = map[string]interface{}{"NULL": *v.NULL}
		} else {
			return nil, fmt.Errorf("unsupported expression attribute type for key %s", k)
		}
	}
	return awsMap, nil
}
