package protocol

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestModelSelectionRejectsPartialBaseProvenance(t *testing.T) {
	raw, err := os.ReadFile("testdata/model_selection/valid_base.json")
	require.NoError(t, err)
	var selection ModelSelection
	require.NoError(t, json.Unmarshal(raw, &selection))
	require.NoError(t, selection.Validate())

	selection.AdapterSHA256 = "0000000000000000000000000000000000000000000000000000000000000000"
	require.Error(t, selection.Validate())
}
