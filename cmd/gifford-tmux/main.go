// Command gifford-tmux is the closed privileged tmux interface.
package main

import (
	"os"

	"github.com/smellak/gifford/internal/tmuxhelper"
)

func main() {
	os.Exit(tmuxhelper.Main(os.Args[1:]))
}
