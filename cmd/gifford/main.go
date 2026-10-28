// Command gifford is the single binary of the Gifford protocol.
package main

import (
	"os"

	"github.com/smellak/gifford/internal/cli"
)

func main() {
	os.Exit(cli.Main())
}
