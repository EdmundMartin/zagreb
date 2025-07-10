package routerapi

type RegisterNodeRequest struct {
	ID   string `json:"id"`
	Addr string `json:"addr"`
}

type DeregisterNodeRequest struct {
	ID string `json:"id"`
}
