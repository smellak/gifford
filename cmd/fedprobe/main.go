// Command fedprobe runs the live federation boundary probe.
package main

import (
	"os"

	"github.com/smellak/gifford/internal/federation"
)

func main() {
	os.Exit(federation.ProbeMain(os.Args[1:], os.Stdout, os.Stderr))
}
