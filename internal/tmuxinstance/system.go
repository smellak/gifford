package tmuxinstance

import (
	"bytes"
	"errors"
	"io/fs"
	"path"
	"regexp"
	"strings"
)

// errProcessGone means the process exited, became a zombie, or its PID now
// belongs to another process.
var errProcessGone = errors.New("process gone")

// procInfo is what /proc shows for one process to any user. Of its
// command line it keeps only what programOf keeps.
type procInfo struct {
	ppid  int64
	start int64
	uid   uint32
	gid   uint32
	euid  uint32
	egid  uint32
	comm  string
	argv0 string
	argv1 string
}

// maxArgvBytes bounds the read of a command line: two paths and their NULs.
const maxArgvBytes = 2*4096 + 2

// interpreterPattern names the interpreters whose program is argv[1].
var interpreterPattern = regexp.MustCompile(`^(node|bun|python(3(\.[0-9]+)?)?)$`)

// programOf keeps only the basename of argv[0] and, when argv[0] is a known
// interpreter, the basename of argv[1]. An argv[1] that starts with '-' is
// an option, where a secret may hide, and is never kept. complete says the
// read was not cut short; an argument the bound cut is not kept. The rest of
// the command line, where other users' secrets may hide, is dropped here.
func programOf(cmdline []byte, complete bool) (argv0, argv1 string) {
	args := bytes.SplitN(cmdline, []byte{0}, 3)
	if !complete && len(args) < 3 {
		args = args[:len(args)-1]
	}
	if len(args) == 0 || len(args[0]) == 0 {
		return "", ""
	}
	argv0 = strings.Clone(path.Base(string(args[0])))
	if !interpreterPattern.MatchString(argv0) || len(args) < 2 || len(args[1]) == 0 || args[1][0] == '-' {
		return argv0, ""
	}
	return argv0, strings.Clone(path.Base(string(args[1])))
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
