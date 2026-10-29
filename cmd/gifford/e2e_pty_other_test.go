//go:build !unix

package main

import (
	"errors"
	"os"
)

func openE2EPTY() (*os.File, *os.File, error) {
	return nil, nil, errors.New("PTY unavailable")
}
