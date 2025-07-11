package routerapi

import (
	"zagreb/pkg/router"
)

type RegisterNodeRequest struct {
	ID   string `json:"id"`
	Addr string `json:"addr"`
}

type DeregisterNodeRequest struct {
	ID string `json:"id"`
}

// RegisterNodeResponse is the response body for registering a node with the router.
type RegisterNodeResponse struct {
	ActiveNodes []router.Node `json:"activeNodes"`
}
