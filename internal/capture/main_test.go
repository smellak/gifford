package capture

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/protocol"
)

// The environment of an import run as a process of its own: the test
// binary runs it instead of the tests.
const (
	childEnv       = "GIFFORD_CAPTURE_TEST_IMPORT"
	childDataEnv   = "GIFFORD_CAPTURE_TEST_DATA"
	childSourceEnv = "GIFFORD_CAPTURE_TEST_SOURCE"
	childAgentEnv  = "GIFFORD_CAPTURE_TEST_AGENT"
	childStartEnv  = "GIFFORD_CAPTURE_TEST_START"
)

func TestMain(m *testing.M) {
	if os.Getenv(childEnv) == "1" {
		os.Exit(importChild())
	}
	os.Exit(m.Run())
}

// importChild imports once, as soon as the start file is there, and
// prints new, already or the error.
func importChild() int {
	for {
		if _, err := os.Stat(os.Getenv(childStartEnv)); err == nil {
			break
		}
		time.Sleep(time.Millisecond)
	}
	res, err := Import(context.Background(), Options{
		DataDir: os.Getenv(childDataEnv), Source: os.Getenv(childSourceEnv), Format: FormatAsciicastV2,
		AgentID: protocol.AgentID(os.Getenv(childAgentEnv)), Harness: protocol.HarnessCodex,
		EnvironmentDomain: dataset.EnvironmentDomainFleetManaged,
	})
	switch {
	case err != nil:
		fmt.Println("error:", err)
	case res.Already:
		fmt.Println("already")
	default:
		fmt.Println("new")
	}
	return 0
}
