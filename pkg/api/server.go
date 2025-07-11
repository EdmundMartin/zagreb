package api

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"

	"github.com/gorilla/mux"
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
		var scanReq types.ScanRequest
		if err := json.Unmarshal(body, &scanReq); err != nil {
			s.writeError(w, err.Error(), http.StatusBadRequest)
			return
		}
		items, err := s.storage.Scan(&scanReq)
		if err != nil {
			s.writeError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(types.ScanResponse{Items: items})
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
	w.WriteHeader(http.StatusOK)
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
