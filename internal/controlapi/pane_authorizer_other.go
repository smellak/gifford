//go:build !linux

package controlapi

func loadPaneAdminUIDs(string) ([]uint32, error) {
	return nil, ErrPaneOperationPolicyInvalid
}
