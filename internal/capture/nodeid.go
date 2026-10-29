package capture

import (
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
)

// nodeIDPattern matches a persisted bootstrap node id: capture- plus
// 32 lowercase hex characters and a newline in the file.
var nodeIDPattern = regexp.MustCompile(`^capture-[0-9a-f]{32}$`)

// newNodeID draws a fresh node identity: capture- plus 32 lowercase
// hex characters from crypto/rand.
func newNodeID() (string, error) {
	nonce, err := newNonce()
	if err != nil {
		return "", fmt.Errorf("node id: %w", err)
	}
	return "capture-" + nonce, nil
}

// readNodeID reads the strictly checked node identity.
func readNodeID(root *os.Root) (string, error) {
	f, err := openChecked(root, "node-id", false)
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()
	data, err := io.ReadAll(io.LimitReader(f, 65))
	if err != nil {
		return "", fmt.Errorf("%w: node-id: %w", ErrUnsafePath, err)
	}
	id, ok := strings.CutSuffix(string(data), "\n")
	if !ok || !nodeIDPattern.MatchString(id) {
		return "", fmt.Errorf("%w: bad node id file", ErrUnsafePath)
	}
	return id, nil
}
