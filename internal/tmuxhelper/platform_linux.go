//go:build linux

package tmuxhelper

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const (
	manifestEtcDirectory = "etc"
	manifestDirectory    = "gifford-tmux"
	manifestFilename     = "manifest.json"
	manifestGroup        = "gifford-tmux"
	tmuxExecutable       = "/usr/bin/tmux"
	processRoot          = "/proc"
	bootIDFile           = "/proc/sys/kernel/random/boot_id"
	socketRoot           = "/tmp"
	maxDiagnostic        = 4 << 10
	anyGroupID           = ^uint32(0)
	tmuxWaitDelay        = 250 * time.Millisecond
)

const (
	terminationNone int32 = iota
	terminationTimeout
	terminationOutput
)

type linuxPlatform struct {
	manifestRoot     string
	manifestEtc      string
	manifestDir      string
	manifestFile     string
	manifestDirUID   uint32
	manifestDirGID   uint32
	manifestFileUID  uint32
	manifestFileGID  uint32
	manifestGroup    string
	socketRoot       string
	socketRootUID    uint32
	socketRootGID    uint32
	tmuxPath         string
	procRoot         string
	bootIDPath       string
	effectiveUID     func() int
	lookupGroup      func(string) (*user.Group, error)
	manifestFileMode uint32
}

func newPlatform() *linuxPlatform {
	return &linuxPlatform{
		manifestRoot: "/", manifestEtc: manifestEtcDirectory,
		manifestDir: manifestDirectory, manifestFile: manifestFilename,
		manifestDirUID: 0, manifestDirGID: 0, manifestFileUID: 0,
		manifestGroup: manifestGroup, socketRoot: socketRoot,
		socketRootUID: 0, socketRootGID: 0, tmuxPath: tmuxExecutable,
		procRoot: processRoot, bootIDPath: bootIDFile,
		effectiveUID: os.Geteuid, lookupGroup: user.LookupGroup, manifestFileMode: 0o440,
	}
}

func prepareProcess() error {
	if err := closeInheritedDescriptors(); err != nil {
		return fmt.Errorf("close inherited descriptors: %w", err)
	}
	if err := unix.Chdir("/"); err != nil {
		return fmt.Errorf("change helper directory: %w", err)
	}
	return nil
}

func closeInheritedDescriptors() error {
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return fmt.Errorf("enumerate descriptors: %w", err)
	}
	for _, entry := range entries {
		fd64, parseErr := strconv.ParseUint(entry.Name(), 10, 32)
		if parseErr != nil || fd64 < 3 {
			continue
		}
		fd := uint(fd64)
		flags, flagErr := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
		if errors.Is(flagErr, unix.EBADF) {
			continue
		}
		if flagErr != nil {
			return fmt.Errorf("inspect descriptor %d: %w", fd, flagErr)
		}
		// Descriptors that survive exec are the unmarked inherited set. Go's
		// runtime descriptors are created after exec with FD_CLOEXEC and must
		// remain open (notably the netpoll epoll descriptor).
		if flags&unix.FD_CLOEXEC != 0 {
			continue
		}
		if closeErr := unix.CloseRange(fd, fd, 0); closeErr != nil && !errors.Is(closeErr, unix.EBADF) {
			return fmt.Errorf("close descriptor %d: %w", fd, closeErr)
		}
	}
	return nil
}

func (p *linuxPlatform) EffectiveUID() uint32 {
	if p == nil || p.effectiveUID == nil {
		return 0
	}
	uid := p.effectiveUID()
	if uid < 0 || uint64(uid) > uint64(^uint32(0)) {
		return 0
	}
	return uint32(uid)
}

func (p *linuxPlatform) LoadManifest() ([]byte, error) {
	if p == nil {
		return nil, errors.New("nil linux platform")
	}
	groupID, err := p.expectedManifestGroup()
	if err != nil {
		return nil, err
	}
	rootFD, err := unix.Open(p.manifestRoot, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("open manifest root: %w", err)
	}
	defer func() { _ = unix.Close(rootFD) }()
	if err := requireTrustedDirectory(rootFD, p.manifestDirUID, p.manifestDirGID); err != nil {
		return nil, fmt.Errorf("validate manifest root: %w", err)
	}
	etcFD, err := openBeneath(rootFD, p.manifestEtc, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC)
	if err != nil {
		return nil, fmt.Errorf("open manifest ancestor: %w", err)
	}
	defer func() { _ = unix.Close(etcFD) }()
	if err := requireTrustedDirectory(etcFD, p.manifestDirUID, p.manifestDirGID); err != nil {
		return nil, fmt.Errorf("validate manifest ancestor: %w", err)
	}
	dirFD, err := openBeneath(etcFD, p.manifestDir, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC)
	if err != nil {
		return nil, fmt.Errorf("open manifest directory: %w", err)
	}
	defer func() { _ = unix.Close(dirFD) }()
	if err := requireTrustedDirectory(dirFD, p.manifestDirUID, p.manifestDirGID); err != nil {
		return nil, fmt.Errorf("validate manifest directory: %w", err)
	}
	if err := requireNoACL(dirFD, true); err != nil {
		return nil, fmt.Errorf("validate manifest directory ACL: %w", err)
	}
	fileFD, err := openBeneath(dirFD, p.manifestFile, unix.O_RDONLY|unix.O_CLOEXEC)
	if err != nil {
		return nil, fmt.Errorf("open manifest: %w", err)
	}
	defer func() { _ = unix.Close(fileFD) }()
	before, err := statFD(fileFD)
	if err != nil {
		return nil, fmt.Errorf("stat manifest: %w", err)
	}
	if err := requireStat(before, unix.S_IFREG, p.manifestFileUID, groupID, p.manifestFileMode, true); err != nil {
		return nil, fmt.Errorf("validate manifest: %w", err)
	}
	if err := requireNoACL(fileFD, false); err != nil {
		return nil, fmt.Errorf("validate manifest ACL: %w", err)
	}
	if before.Size < 1 || before.Size > maxManifestBytes {
		return nil, errors.New("manifest size is outside its bound")
	}
	raw, err := readBoundedFD(fileFD, maxManifestBytes)
	if err != nil {
		return nil, fmt.Errorf("read manifest: %w", err)
	}
	after, err := statFD(fileFD)
	if err != nil {
		return nil, fmt.Errorf("restat manifest: %w", err)
	}
	if !sameStat(before, after) || int64(len(raw)) != before.Size {
		return nil, errors.New("manifest changed while it was read")
	}
	return raw, nil
}

func (p *linuxPlatform) expectedManifestGroup() (uint32, error) {
	if p.manifestGroup == "" {
		return p.manifestFileGID, nil
	}
	if p.lookupGroup == nil {
		return 0, errors.New("group lookup is unavailable")
	}
	group, err := p.lookupGroup(p.manifestGroup)
	if err != nil {
		return 0, fmt.Errorf("lookup manifest group: %w", err)
	}
	gid, err := strconv.ParseUint(group.Gid, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("parse manifest group: %w", err)
	}
	return uint32(gid), nil
}

func (p *linuxPlatform) ValidateSocket(uid uint32, socket string) error {
	if p == nil || uid == 0 || !validSocketPath(p.socketRoot, uid, socket) {
		return errors.New("invalid registered socket")
	}
	rootFD, err := unix.Open(p.socketRoot, unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("open socket root: %w", err)
	}
	defer func() { _ = unix.Close(rootFD) }()
	if err := requireEntry(rootFD, unix.S_IFDIR, p.socketRootUID, p.socketRootGID, 0o1777, false); err != nil {
		return fmt.Errorf("validate socket root: %w", err)
	}
	directory := "tmux-" + strconv.FormatUint(uint64(uid), 10)
	dirFD, err := openBeneath(rootFD, directory, unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC)
	if err != nil {
		return fmt.Errorf("open socket directory: %w", err)
	}
	defer func() { _ = unix.Close(dirFD) }()
	if err := requireEntry(dirFD, unix.S_IFDIR, uid, anyGroupID, 0o700, false); err != nil {
		return fmt.Errorf("validate socket directory: %w", err)
	}
	name := filepath.Base(socket)
	socketFD, err := openBeneath(dirFD, name, unix.O_PATH|unix.O_CLOEXEC)
	if err != nil {
		return fmt.Errorf("open socket: %w", err)
	}
	defer func() { _ = unix.Close(socketFD) }()
	var info unix.Stat_t
	if err := unix.Fstat(socketFD, &info); err != nil {
		return fmt.Errorf("stat socket: %w", err)
	}
	if info.Mode&unix.S_IFMT != unix.S_IFSOCK || info.Uid != uid {
		return errors.New("registered socket identity mismatch")
	}
	return nil
}

func (p *linuxPlatform) RunTmux(
	ctx context.Context,
	socket string,
	args []string,
	limit int,
) ([]byte, ExitCode) {
	if p == nil || ctx == nil || limit < 0 || p.tmuxPath != tmuxExecutable ||
		!trustedExecutable(p.tmuxPath) {
		return nil, ExitEnvironment
	}
	return runTmuxCommand(ctx, p.tmuxPath, socket, args, limit)
}

func runTmuxCommand(
	ctx context.Context,
	executable string,
	socket string,
	args []string,
	limit int,
) ([]byte, ExitCode) {
	if ctx == nil || executable == "" || socket == "" || limit < 0 {
		return nil, ExitEnvironment
	}
	if ctx.Err() != nil {
		return nil, ExitTimeout
	}
	callCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var termination atomic.Int32
	markOutputLimit := func() {
		if termination.CompareAndSwap(terminationNone, terminationOutput) {
			cancel()
		}
	}

	argv := make([]string, 0, 2+len(args))
	argv = append(argv, "-S", socket)
	argv = append(argv, args...)
	// #nosec G204 -- the production caller pins the trusted executable; the
	// seam exists only so tests can exercise the real child-process boundary.
	command := exec.CommandContext(callCtx, executable, argv...)
	command.Env = []string{"LC_ALL=C.UTF-8"}
	command.Dir = "/"
	command.Stdin = nil
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.WaitDelay = tmuxWaitDelay
	command.Cancel = func() error {
		// CommandContext invokes Cancel for a parent deadline or for the
		// output-limit cancellation above. The first cause wins.
		termination.CompareAndSwap(terminationNone, terminationTimeout)
		if command.Process == nil {
			return nil
		}
		if probeErr := command.Process.Signal(syscall.Signal(0)); probeErr != nil {
			if errors.Is(probeErr, os.ErrProcessDone) {
				return os.ErrProcessDone
			}
			return fmt.Errorf("probe tmux process: %w", probeErr)
		}
		err := unix.Kill(-command.Process.Pid, unix.SIGKILL)
		if errors.Is(err, unix.ESRCH) {
			return os.ErrProcessDone
		}
		return fmt.Errorf("kill tmux process group: %w", err)
	}
	stdout := &limitedBuffer{limit: limit, onOverflow: markOutputLimit}
	stderr := &limitedBuffer{limit: maxDiagnostic, onOverflow: markOutputLimit}
	command.Stdout = stdout
	command.Stderr = stderr
	err := command.Run()
	if ctx.Err() != nil {
		termination.CompareAndSwap(terminationNone, terminationTimeout)
	}
	switch termination.Load() {
	case terminationOutput:
		return nil, ExitOutputLimit
	case terminationTimeout:
		return nil, ExitTimeout
	}
	if stdout.overflow || stderr.overflow {
		return nil, ExitOutputLimit
	}
	if err != nil || len(stderr.Bytes()) != 0 {
		return nil, ExitTmux
	}
	return bytes.Clone(stdout.Bytes()), ExitOK
}

func (p *linuxPlatform) ProcessStart(pid int64) (uint64, error) {
	if p == nil || pid < 1 {
		return 0, errors.New("invalid process id")
	}
	path := filepath.Join(p.procRoot, strconv.FormatInt(pid, 10), "stat")
	raw, err := readBoundedFile(path, 16<<10)
	if err != nil {
		return 0, fmt.Errorf("read process identity: %w", err)
	}
	start, ok := parseProcessStart(raw)
	if !ok {
		return 0, errors.New("invalid process identity")
	}
	return start, nil
}

func (p *linuxPlatform) BootID() (string, error) {
	if p == nil {
		return "", errors.New("nil linux platform")
	}
	raw, err := readBoundedFile(p.bootIDPath, 128)
	if err != nil {
		return "", fmt.Errorf("read boot identity: %w", err)
	}
	value := strings.TrimSuffix(string(raw), "\n")
	if !validBootID(value) {
		return "", errors.New("invalid boot identity")
	}
	return value, nil
}

func openBeneath(parent int, path string, flags uint64) (int, error) {
	if path == "" || filepath.IsAbs(path) || filepath.Clean(path) != path {
		return -1, unix.EINVAL
	}
	how := &unix.OpenHow{
		Flags: flags,
		Resolve: unix.RESOLVE_BENEATH | unix.RESOLVE_NO_MAGICLINKS |
			unix.RESOLVE_NO_SYMLINKS,
	}
	fd, err := unix.Openat2(parent, path, how)
	if err != nil {
		return -1, fmt.Errorf("open beneath trusted descriptor: %w", err)
	}
	return fd, nil
}

func requireTrustedDirectory(fd int, uid uint32, gid uint32) error {
	info, err := statFD(fd)
	if err != nil {
		return err
	}
	if info.Mode&unix.S_IFMT != unix.S_IFDIR || info.Uid != uid || info.Gid != gid ||
		info.Mode&0o022 != 0 {
		return fmt.Errorf(
			"trusted directory identity mismatch: kind=%#o uid=%d gid=%d mode=%#o",
			info.Mode&unix.S_IFMT, info.Uid, info.Gid, info.Mode&0o7777,
		)
	}
	return nil
}

func requireNoACL(fd int, directory bool) error {
	names, err := listXattrs(fd)
	if err != nil {
		return err
	}
	for _, name := range names {
		if name == "system.posix_acl_access" || directory && name == "system.posix_acl_default" {
			return errors.New("extended ACL is not allowed")
		}
	}
	return nil
}

func listXattrs(fd int) ([]string, error) {
	for range 3 {
		size, err := unix.Flistxattr(fd, nil)
		if err != nil {
			return nil, fmt.Errorf("size extended attributes: %w", err)
		}
		if size == 0 {
			return nil, nil
		}
		buffer := make([]byte, size)
		read, err := unix.Flistxattr(fd, buffer)
		if errors.Is(err, unix.ERANGE) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("read extended attributes: %w", err)
		}
		parts := bytes.Split(buffer[:read], []byte{0})
		names := make([]string, 0, len(parts))
		for _, part := range parts {
			if len(part) != 0 {
				names = append(names, string(part))
			}
		}
		return names, nil
	}
	return nil, errors.New("extended attributes changed during inspection")
}

func requireEntry(
	fd int,
	kind uint32,
	uid uint32,
	gid uint32,
	mode uint32,
	oneLink bool,
) error {
	info, err := statFD(fd)
	if err != nil {
		return err
	}
	return requireStat(info, kind, uid, gid, mode, oneLink)
}

func requireStat(
	info unix.Stat_t,
	kind uint32,
	uid uint32,
	gid uint32,
	mode uint32,
	oneLink bool,
) error {
	if info.Mode&unix.S_IFMT != kind || info.Uid != uid || gid != anyGroupID && info.Gid != gid ||
		info.Mode&0o7777 != mode || oneLink && info.Nlink != 1 {
		return errors.New("filesystem identity mismatch")
	}
	return nil
}

func statFD(fd int) (unix.Stat_t, error) {
	var info unix.Stat_t
	if err := unix.Fstat(fd, &info); err != nil {
		return unix.Stat_t{}, fmt.Errorf("stat descriptor: %w", err)
	}
	return info, nil
}

func sameStat(left, right unix.Stat_t) bool {
	return left.Dev == right.Dev && left.Ino == right.Ino && left.Mode == right.Mode &&
		left.Uid == right.Uid && left.Gid == right.Gid && left.Nlink == right.Nlink &&
		left.Size == right.Size && left.Mtim == right.Mtim && left.Ctim == right.Ctim
}

func readBoundedFD(fd int, limit int) ([]byte, error) {
	raw := make([]byte, 0, min(limit, 4096))
	buffer := make([]byte, min(limit+1, 4096))
	for offset := int64(0); ; {
		read, err := unix.Pread(fd, buffer, offset)
		if read > 0 {
			raw = append(raw, buffer[:read]...)
			offset += int64(read)
			if len(raw) > limit {
				return nil, errors.New("file exceeds its size bound")
			}
		}
		if err != nil && !errors.Is(err, unix.EINTR) {
			return nil, fmt.Errorf("read bounded descriptor: %w", err)
		}
		if read == 0 {
			return raw, nil
		}
	}
}

func readBoundedFile(path string, limit int) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open bounded file: %w", err)
	}
	defer func() { _ = file.Close() }()
	raw, err := io.ReadAll(io.LimitReader(file, int64(limit)+1))
	if err != nil {
		return nil, fmt.Errorf("read bounded file: %w", err)
	}
	if len(raw) > limit {
		return nil, errors.New("file exceeds its size bound")
	}
	return raw, nil
}

func parseProcessStart(raw []byte) (uint64, bool) {
	closing := bytes.LastIndexByte(raw, ')')
	if closing < 1 || closing+2 >= len(raw) || raw[closing+1] != ' ' {
		return 0, false
	}
	fields := bytes.Fields(raw[closing+2:])
	// After comm, fields begins with process field 3. starttime is field 22.
	if len(fields) < 20 {
		return 0, false
	}
	start := string(fields[19])
	parsed, err := strconv.ParseUint(start, 10, 64)
	return parsed, err == nil && strconv.FormatUint(parsed, 10) == start
}

func trustedExecutable(path string) bool {
	for _, directory := range []string{"/", "/usr", "/usr/bin"} {
		info, err := os.Lstat(directory)
		statInfo, ok := infoSysStat(info)
		if err != nil || !ok || !info.IsDir() || statInfo.Uid != 0 || info.Mode().Perm()&0o022 != 0 {
			return false
		}
	}
	info, err := os.Lstat(path)
	statInfo, ok := infoSysStat(info)
	return err == nil && ok && info.Mode().IsRegular() && statInfo.Uid == 0 &&
		info.Mode().Perm()&0o022 == 0 && info.Mode().Perm()&0o111 != 0
}

func infoSysStat(info os.FileInfo) (*syscall.Stat_t, bool) {
	if info == nil {
		return nil, false
	}
	value, ok := info.Sys().(*syscall.Stat_t)
	return value, ok
}

type limitedBuffer struct {
	buffer     bytes.Buffer
	limit      int
	overflow   bool
	onOverflow func()
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	written := len(p)
	remaining := b.limit - b.buffer.Len()
	if remaining > 0 {
		keep := min(len(p), remaining)
		_, _ = b.buffer.Write(p[:keep])
	}
	if len(p) > remaining {
		b.overflow = true
		if b.onOverflow != nil {
			b.onOverflow()
		}
	}
	return written, nil
}

func (b *limitedBuffer) Bytes() []byte { return b.buffer.Bytes() }
