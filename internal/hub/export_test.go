package hub

// NewNodeSocketForNode lets the external tests in this directory serve
// node.sock as if the node ran under nodeUID, so the test process can
// connect as an agent of another user.
func NewNodeSocketForNode(server *Server, path string, bridgeUID, nodeUID int64) (*NodeSocket, error) {
	return newNodeSocket(server, path, bridgeUID, nodeUID)
}
