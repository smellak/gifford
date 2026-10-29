//go:build unix

package main

import (
	"os"

	"github.com/creack/pty"
)

func openE2EPTY() (*os.File, *os.File, error) {
	outer, slave, err := pty.Open()
	if err != nil {
		return nil, nil, err
	}
	if err := pty.Setsize(outer, &pty.Winsize{Rows: 24, Cols: 120}); err != nil {
		_ = outer.Close()
		_ = slave.Close()
		return nil, nil, err
	}
	return outer, slave, nil
}
