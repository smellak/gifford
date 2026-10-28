package main

import "github.com/smellak/gifford/internal/channel"

func main() {
	var d channel.Decision
	_ = d.Allowed()
	_ = d.Outcome()
}
