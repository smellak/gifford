package tmuxhelper

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"

	"github.com/smellak/gifford/internal/tmuxnotice"
)

const manifestVersion = 1

// Registry is the canonical, root-owned authority shared by the node and the
// privileged helper. Concrete registry contents are deployment-private.
type Registry struct {
	Banks         map[string]Bank `json:"banks"`
	NoticeFull    string          `json:"notice_full"`
	NoticeShort   string          `json:"notice_short"`
	SchemaVersion int             `json:"schema_version"`
}

// Bank binds one closed identifier to an operating-system owner and tmux
// socket.
type Bank struct {
	Panes  map[string]PaneBinding `json:"panes"`
	Socket string                 `json:"socket"`
	UID    uint32                 `json:"uid"`
}

// PaneBinding pins a tmux pane to one Linux process incarnation.
type PaneBinding struct {
	BootID    string `json:"boot_id"`
	PanePID   int64  `json:"pane_pid"`
	PaneStart uint64 `json:"pane_start"`
}

// ParseRegistry accepts only the exact canonical JSON representation. Parsing
// and re-encoding before comparing rejects duplicate keys, alternate escapes,
// whitespace, reordered keys, and trailing data without a second parser.
func ParseRegistry(raw []byte) (Registry, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var registry Registry
	if err := decoder.Decode(&registry); err != nil {
		return Registry{}, fmt.Errorf("decode registry: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("multiple JSON values")
		}
		return Registry{}, fmt.Errorf("decode registry: %w", err)
	}
	if err := registry.validate(); err != nil {
		return Registry{}, err
	}
	if !bytes.Equal(raw, registry.canonicalJSON()) {
		return Registry{}, errors.New("registry is not canonical JSON")
	}
	return registry, nil
}

func (r Registry) validate() error {
	if r.SchemaVersion != manifestVersion || len(r.Banks) == 0 ||
		!tmuxnotice.Valid(r.NoticeFull) || !tmuxnotice.Valid(r.NoticeShort) ||
		r.NoticeFull == r.NoticeShort {
		return errors.New("registry contract mismatch")
	}
	for id, bank := range r.Banks {
		if !validBankID(id) || bank.UID == 0 || bank.Panes == nil ||
			!validSocketPath("/tmp", bank.UID, bank.Socket) {
			return errors.New("invalid bank registration")
		}
		for pane, binding := range bank.Panes {
			if !canonicalPane(pane) || !binding.valid() {
				return errors.New("invalid pane registration")
			}
		}
	}
	return nil
}

func (p PaneBinding) valid() bool {
	return p.PanePID > 0 && validBootID(p.BootID)
}

func validBankID(value string) bool {
	if len(value) < 1 || len(value) > 32 {
		return false
	}
	for index := range len(value) {
		char := value[index]
		if char != '-' && (char < 'a' || char > 'z') && (char < '0' || char > '9') {
			return false
		}
	}
	return true
}

func canonicalPane(pane string) bool {
	if pane == "%0" {
		return true
	}
	if len(pane) < 2 || pane[0] != '%' || pane[1] < '1' || pane[1] > '9' {
		return false
	}
	for index := 2; index < len(pane); index++ {
		if pane[index] < '0' || pane[index] > '9' {
			return false
		}
	}
	return true
}

func validBootID(value string) bool {
	if len(value) != 36 {
		return false
	}
	for index := range len(value) {
		switch index {
		case 8, 13, 18, 23:
			if value[index] != '-' {
				return false
			}
		default:
			char := value[index]
			if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
				return false
			}
		}
	}
	return true
}

func validSocketPath(root string, uid uint32, socket string) bool {
	directory := root + "/tmux-" + strconv.FormatUint(uint64(uid), 10)
	if len(socket) < len(directory)+2 || len(socket) > maxSocketPathBytes ||
		!strings.HasPrefix(socket, directory+"/") {
		return false
	}
	name := strings.TrimPrefix(socket, directory+"/")
	if len(name) < 1 || len(name) > 64 {
		return false
	}
	for index := range len(name) {
		char := name[index]
		if char != '_' && char != '-' && (char < 'A' || char > 'Z') &&
			(char < 'a' || char > 'z') && (char < '0' || char > '9') {
			return false
		}
	}
	return true
}

func (r Registry) canonicalJSON() []byte {
	result := make([]byte, 0, 1024)
	result = append(result, `{"banks":{`...)
	bankIDs := sortedKeys(r.Banks)
	for index, id := range bankIDs {
		if index > 0 {
			result = append(result, ',')
		}
		result = appendJSONString(result, id)
		result = append(result, `:{"panes":{`...)
		bank := r.Banks[id]
		paneIDs := sortedKeys(bank.Panes)
		for paneIndex, pane := range paneIDs {
			if paneIndex > 0 {
				result = append(result, ',')
			}
			result = appendJSONString(result, pane)
			binding := bank.Panes[pane]
			result = append(result, `:{"boot_id":`...)
			result = appendJSONString(result, binding.BootID)
			result = append(result, `,"pane_pid":`...)
			result = strconv.AppendInt(result, binding.PanePID, 10)
			result = append(result, `,"pane_start":`...)
			result = strconv.AppendUint(result, binding.PaneStart, 10)
			result = append(result, '}')
		}
		result = append(result, `},"socket":`...)
		result = appendJSONString(result, bank.Socket)
		result = append(result, `,"uid":`...)
		result = strconv.AppendUint(result, uint64(bank.UID), 10)
		result = append(result, '}')
	}
	result = append(result, `},"notice_full":`...)
	result = appendJSONString(result, r.NoticeFull)
	result = append(result, `,"notice_short":`...)
	result = appendJSONString(result, r.NoticeShort)
	result = append(result, `,"schema_version":`...)
	result = strconv.AppendInt(result, int64(r.SchemaVersion), 10)
	result = append(result, '}')
	return result
}

func sortedKeys[T any](values map[string]T) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func appendJSONString(output []byte, value string) []byte {
	output = append(output, '"')
	for index := range len(value) {
		switch value[index] {
		case '"', '\\':
			output = append(output, '\\', value[index])
		default:
			output = append(output, value[index])
		}
	}
	return append(output, '"')
}
