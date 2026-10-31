package nodeproc

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
)

// runs reports whether process pid runs exactly command, from its command
// line in /proc. A process that is gone runs nothing.
func runs(pid int, command []string) (bool, error) {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/cmdline")
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read the command line of PID %d: %w", pid, err)
	}
	args := bytes.Split(bytes.TrimSuffix(b, []byte{0}), []byte{0})
	return slices.EqualFunc(args, command, func(a []byte, c string) bool { return string(a) == c }), nil
}
