package protocol

import (
	"path/filepath"
)

// PaneAttemptID is either the no-attempt sentinel or one exact lower-case
// hexadecimal attempt identity.
type PaneAttemptID string

// JSONPattern returns the ECMA-262 pattern of a pane attempt identity.
func (PaneAttemptID) JSONPattern() string { return `^(-|[0-9a-f]{32})$` }

// Valid reports whether the attempt identity is canonical.
func (id PaneAttemptID) Valid() bool {
	if id == "-" {
		return true
	}
	if len(id) != 32 {
		return false
	}
	for _, char := range id {
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return false
		}
	}
	return true
}

// PaneOperationTarget is the complete live identity of one pane. Every field
// participates in authority; callers cannot select a pane by its reusable id
// alone.
type PaneOperationTarget struct {
	OwnerUID  uint32     `json:"owner_uid" jsonschema:"minimum=1"`
	Socket    string     `json:"socket" jsonschema:"pattern=^/[!-~]+$,minLength=2,maxLength=107"`
	PaneID    TmuxPaneID `json:"pane_id" jsonschema:"pattern=^%(0|[1-9][0-9]*)$,maxLength=10"`
	Host      string     `json:"host" jsonschema:"pattern=^[a-z0-9-]+$,minLength=1,maxLength=63"`
	BootID    string     `json:"boot_id" jsonschema:"pattern=^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"`
	PanePID   int64      `json:"pane_pid" jsonschema:"minimum=1,maximum=4194304"`
	PaneStart int64      `json:"pane_start" jsonschema:"minimum=1"`
}

// Validate checks canonical live identity syntax before any administrative
// backend or policy file is touched.
func (target PaneOperationTarget) Validate() error {
	if err := checkRoot(&target); err != nil {
		return err
	}
	if target.OwnerUID == 0 || target.PanePID <= 0 || target.PanePID > 1<<22 ||
		target.PaneStart <= 0 || !canonicalPaneOperationID(target.PaneID) ||
		!paneOperationHostValid(target.Host) || !paneOperationBootIDValid(target.BootID) ||
		!paneOperationSocketValid(target.Socket) {
		return ErrInvalid
	}
	return nil
}

// PaneOperationParams are shared by provision, take and return.
type PaneOperationParams struct {
	Target PaneOperationTarget `json:"target"`
}

// Validate checks the complete target.
func (params PaneOperationParams) Validate() error {
	if err := checkRoot(&params); err != nil {
		return err
	}
	return params.Target.Validate()
}

// PaneAttemptParams make reconcile conditional on the exact state an operator
// inspected. Repair accepts PaneOperationParams because corrupt state may not
// expose a trustworthy attempt identity.
type PaneAttemptParams struct {
	Target          PaneOperationTarget `json:"target"`
	ExpectedAttempt PaneAttemptID       `json:"expected_attempt"`
}

// Validate checks the complete target and expected attempt identity.
func (params PaneAttemptParams) Validate() error {
	if err := checkRoot(&params); err != nil {
		return err
	}
	if err := params.Target.Validate(); err != nil {
		return err
	}
	if !params.ExpectedAttempt.Valid() {
		return ErrInvalid
	}
	return nil
}

// PaneOperationResult is the common idempotent acknowledgement for all pane
// administrative operations.
type PaneOperationResult struct {
	Changed bool `json:"changed"`
}

// Validate checks the result root.
func (result PaneOperationResult) Validate() error { return checkRoot(&result) }

func canonicalPaneOperationID(id TmuxPaneID) bool {
	return id.Valid() && (len(id) == 2 || id[1] != '0')
}

func paneOperationHostValid(value string) bool {
	if len(value) == 0 || len(value) > 63 {
		return false
	}
	for _, char := range value {
		if char != '-' && (char < 'a' || char > 'z') && (char < '0' || char > '9') {
			return false
		}
	}
	return true
}

func paneOperationBootIDValid(value string) bool {
	if len(value) != 36 || value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' {
		return false
	}
	for index, char := range value {
		if index == 8 || index == 13 || index == 18 || index == 23 {
			continue
		}
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return false
		}
	}
	return true
}

func paneOperationSocketValid(value string) bool {
	if len(value) < 2 || len(value) > 107 || value[0] != '/' || filepath.Clean(value) != value {
		return false
	}
	for index := 1; index < len(value); index++ {
		if value[index] < '!' || value[index] > '~' {
			return false
		}
	}
	return true
}
