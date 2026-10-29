package protocol

// NodeEnrollTokenParams requests a one-use enrollment token. An empty
// organization_id means the local configured organization; ttl_ms absent means
// the 15-minute default.
type NodeEnrollTokenParams struct {
	OrganizationID string `json:"organization_id,omitempty" jsonschema:"maxLength=64"`
	TTLMS          *int64 `json:"ttl_ms,omitempty" jsonschema:"minimum=1,maximum=900000"`
}

// Validate checks the optional organization uses the node-id character set.
func (p NodeEnrollTokenParams) Validate() error {
	if err := checkRoot(&p); err != nil {
		return err
	}
	if p.OrganizationID != "" && !NodeID(p.OrganizationID).Valid() {
		return &ValidationError{Path: "organization_id", Reason: "invalid organization id"}
	}
	return nil
}

// NodeEnrollTokenResult is printed exactly once by the CLI.
type NodeEnrollTokenResult struct {
	Token       string    `json:"token"`
	NodeID      NodeID    `json:"node_id"`
	Endpoint    string    `json:"endpoint"`
	Fingerprint SHA256Hex `json:"fingerprint"`
	ExpiresAt   string    `json:"expires_at"`
}

// NodeJoinParams supplies the accepting node URL, one-use token, and the
// certificate fingerprint confirmed out of band.
type NodeJoinParams struct {
	URL         string    `json:"url" jsonschema:"minLength=1,maxLength=2048"`
	Token       string    `json:"token" jsonschema:"minLength=1,maxLength=128"`
	Fingerprint SHA256Hex `json:"fingerprint"`
}

// Validate checks join parameter shapes. URL semantics are checked by the
// federation client before opening a connection.
func (p NodeJoinParams) Validate() error { return checkRoot(&p) }

// NodePeer is one peer exposed to the local node CLI.
type NodePeer struct {
	NodeID         NodeID    `json:"node_id"`
	OrganizationID string    `json:"organization_id" jsonschema:"minLength=1,maxLength=64"`
	Status         string    `json:"status" jsonschema:"enum=TRUSTED,enum=SUSPENDED,enum=REVOKED"`
	Endpoint       string    `json:"endpoint,omitempty"`
	Fingerprint    SHA256Hex `json:"fingerprint,omitempty"`
	PolicyVersion  string    `json:"policy_version" jsonschema:"minLength=22,maxLength=22"`
}

// NodeJoinResult identifies the accepting peer now pinned locally.
type NodeJoinResult struct {
	Peer NodePeer `json:"peer"`
}

// NodePeersResult lists peers in stable node-id order.
type NodePeersResult struct {
	Peers []NodePeer `json:"peers"`
}

// NodeRevokeParams names the peer whose trust is revoked.
type NodeRevokeParams struct {
	NodeID NodeID `json:"node_id"`
}

// Validate checks the peer id.
func (p NodeRevokeParams) Validate() error { return checkRoot(&p) }

// NodeRevokeResult returns the peer's terminal state.
type NodeRevokeResult struct {
	Peer NodePeer `json:"peer"`
}
