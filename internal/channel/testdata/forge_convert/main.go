package main

import "github.com/smellak/gifford/internal/channel"

type lookalike struct{ d *struct{} }

func main() {
	_ = channel.Decision(lookalike{})
}
