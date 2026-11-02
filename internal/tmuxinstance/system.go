package tmuxinstance

import (
	"errors"
	"io/fs"
)

// errProcessGone means the process exited, became a zombie, or its PID now
// belongs to another process.
var errProcessGone = errors.New("process gone")

// procInfo is what /proc shows for one process to any user.
type procInfo struct {
	ppid  int64
	start int64
	uid   uint32
	gid   uint32
	comm  string
}

// dirInfo is the lstat of a tmux socket's directory. The node cannot look
// inside it: tmux keeps it 0700 for the server's owner.
type dirInfo struct {
	uid     uint32
	perm    fs.FileMode
	dir     bool
	symlink bool
}

// system is the machine state the registry reads. Tests replace it.
type system interface {
	bootID() (string, error)
	machineID() (string, error)
	process(pid int64) (procInfo, error)
	children(pid int64) ([]int64, error)
	socketDir(socket string) (dirInfo, error)
	lookupUser(name string) (uint32, error)
}

func isNotExist(err error) bool { return errors.Is(err, fs.ErrNotExist) }
