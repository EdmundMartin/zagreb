package api

import (
	"log"
	"net/http"

	"github.com/gorilla/mux"
	"zagreb/pkg/storage"
)

// Server represents the HTTP API server.
type Server struct {
	storage storage.Storage
	router  *mux.Router
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

