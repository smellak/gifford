//go:build linux

package adapter

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"

	"github.com/smellak/gifford/internal/nativeevents"
	"github.com/smellak/gifford/internal/protocol"
)

const (
	museParentTransitions = 2
	museMaxFDEntries      = 256
	museMaxFDLinkBytes    = 128
	museMaxProcStatBytes  = 8 << 10
	museMaxProcUnixBytes  = 4 << 20
	museListenFlag        = 0x00010000
	museSocketStream      = 0x0001
	museSocketUnconnected = 0x01
	museMarkerReasonLimit = 16

	museAgentsDirectory      = "agents"
	museNativeDirectory      = "native-events"
	museDiagnosticDirectory  = "hook-diagnostics"
	museDiagnosticMuseFolder = "muse"
)

type museMarkerReason string

const (
	museReasonNativeMissing   museMarkerReason = "native_missing"
	museReasonNativeAmbiguous museMarkerReason = "native_ambiguous"
	museReasonNativeUnsafe    museMarkerReason = "native_unsafe"
	museReasonConnect         museMarkerReason = "connect_failed"
	museReasonPeer            museMarkerReason = "peer_mismatch"
	museReasonProtocol        museMarkerReason = "protocol_failed"
)

var museMarkerReasons = [...]museMarkerReason{
	museReasonNativeMissing,
	museReasonNativeAmbiguous,
	museReasonNativeUnsafe,
	museReasonConnect,
	museReasonPeer,
	museReasonProtocol,
}

var errMuseNativeRouteChanged = errors.New("muse socket: native route changed")

// errMuseNoSocketFDs marks a boundary that holds no sockets at all: a
// shell running Muse from its prompt, unlike a measured run, which always
// holds its listening sockets.
var errMuseNoSocketFDs = errors.New("muse socket: no socket fds")

type museProcessToken struct {
	pid    int
	parent int
	start  uint64
}

type museSocketCandidate struct {
	path string
}

type museSocketCandidates struct {
	agents []museSocketCandidate
	native []museSocketCandidate
}

type museFileIdentity struct {
	device uint64
	inode  uint64
}

type museAnchor struct {
	run                     museProcessToken
	runtime                 string
	agent                   protocol.AgentID
	runtimeFD               int
	runtimeIdentity         museFileIdentity
	nativeDirectoryIdentity museFileIdentity
	nativeSocketIdentity    museFileIdentity
	nativeSocketFD          int
	// systemNative marks an anchor bound to the documented system node
	// entry instead of a measured per-run topology. Its route is
	// revalidated from the retained socket alone, and its peer is the
	// node server, never a run process.
	systemNative bool
	// systemPeerUID is the owner of the system socket at bind time. The
	// dialed peer must run as that same uid.
	systemPeerUID uint32
}

func (a *museAnchor) close() {
	if a != nil && a.nativeSocketFD >= 0 {
		_ = unix.Close(a.nativeSocketFD)
		a.nativeSocketFD = -1
	}
	if a != nil && a.runtimeFD >= 0 {
		_ = unix.Close(a.runtimeFD)
		a.runtimeFD = -1
	}
}

type museResolution struct {
	anchor *museAnchor
}

type museResolver struct {
	procRoot string
	pid      int
	euid     int
	dial     func(context.Context, string) (net.Conn, error)
}

func emitMuseFallback(
	ctx context.Context,
	hello nativeevents.Hello,
	event nativeevents.Event,
) error {
	dialer := &net.Dialer{}
	resolver := museResolver{
		procRoot: "/proc",
		pid:      os.Getpid(),
		euid:     os.Geteuid(),
		dial: func(ctx context.Context, path string) (net.Conn, error) {
			return dialer.DialContext(ctx, "unix", path)
		},
	}
	return resolver.emit(ctx, hello, event)
}

func (r museResolver) emit(
	ctx context.Context,
	hello nativeevents.Hello,
	event nativeevents.Event,
) error {
	resolution, reason, err := r.resolve(ctx)
	if resolution.anchor != nil {
		defer resolution.anchor.close()
	}
	if err != nil {
		r.mark(ctx, resolution.anchor, reason)
		return err
	}
	for attempt := range 2 {
		reason, err = r.emitAttempt(ctx, resolution.anchor, hello, event)
		if err == nil {
			return nil
		}
		if attempt == 0 && retryIncompleteHello(err) {
			continue
		}
		r.mark(ctx, resolution.anchor, r.reason(ctx, err, reason))
		return err
	}
	return err
}

func (r museResolver) emitAttempt(
	ctx context.Context,
	anchor *museAnchor,
	hello nativeevents.Hello,
	event nativeevents.Event,
) (museMarkerReason, error) {
	if err := r.revalidate(anchor.run); err != nil {
		return museReasonProtocol, err
	}
	// The retained FD prevents route changes from redirecting the connection.
	// A change can still make the exchange unavailable, and revalidation does
	// not claim to detect a transient move restored before its next check.
	conn, err := r.dial(ctx, "/proc/self/fd/"+strconv.Itoa(anchor.nativeSocketFD))
	if err != nil {
		return museReasonConnect, fmt.Errorf("muse socket: connect: %w", err)
	}
	defer func() { _ = conn.Close() }()
	if err := r.revalidateNativeRoute(anchor); err != nil {
		return museReasonNativeUnsafe, err
	}
	if err := r.authenticatePeer(conn, anchor); err != nil {
		return museReasonPeer, err
	}
	err = emitConnected(ctx, conn, hello, event, func() error {
		if err := r.revalidate(anchor.run); err != nil {
			return err
		}
		return r.revalidateNativeRoute(anchor)
	})
	if err != nil {
		reason := museReasonProtocol
		if errors.Is(err, errMuseNativeRouteChanged) {
			reason = museReasonNativeUnsafe
		}
		return reason, err
	}
	return "", nil
}

func (r museResolver) reason(
	ctx context.Context,
	err error,
	fallback museMarkerReason,
) museMarkerReason {
	if ctx.Err() != nil {
		return ""
	}
	var networkError net.Error
	if errors.Is(err, context.DeadlineExceeded) ||
		(errors.As(err, &networkError) && networkError.Timeout()) {
		return ""
	}
	return fallback
}

func (r museResolver) resolve(
	ctx context.Context,
) (museResolution, museMarkerReason, error) {
	run, err := r.boundary(ctx)
	if err != nil {
		return museResolution{}, "", err
	}
	inodes, err := r.socketInodes(ctx, run)
	if err != nil {
		if !errors.Is(err, errMuseNoSocketFDs) {
			return museResolution{}, "", err
		}
		outerRun, outerErr := r.parentHasMuseTopology(ctx, run)
		if outerErr != nil {
			return museResolution{}, "", outerErr
		}
		if outerRun {
			return museResolution{}, "", errors.New("muse socket: agent anchor unavailable")
		}
		// A socket-free boundary whose parent has no measured topology is
		// a shell invocation, so the system entry is tried below. An inner
		// run never borrows its measured parent's sockets as a route.
		inodes = map[uint64]struct{}{}
	}
	candidates, err := r.socketCandidates(ctx, inodes)
	if err != nil {
		return museResolution{}, "", err
	}
	if len(candidates.agents) == 0 {
		// No measured run topology: a resumed session without a run. Fall
		// back to the hardened system entry, never to a found-but-invalid
		// anchor, which keeps failing closed below.
		anchor := &museAnchor{run: run, runtimeFD: -1, nativeSocketFD: -1}
		if err := r.authenticateSystemNative(anchor); err != nil {
			return museResolution{}, "", errors.New("muse socket: agent anchor unavailable")
		}
		if err := r.revalidate(run); err != nil {
			return museResolution{}, "", err
		}
		return museResolution{anchor: anchor}, "", nil
	}
	if len(candidates.agents) != 1 {
		return museResolution{}, "", errors.New("muse socket: agent anchor unavailable")
	}
	anchor, err := r.authenticateAgent(run, candidates.agents[0])
	if err != nil {
		return museResolution{}, "", err
	}
	resolution := museResolution{anchor: anchor}
	if len(candidates.native) == 0 {
		return resolution, museReasonNativeMissing, errors.New("muse socket: native entry unavailable")
	}
	if len(candidates.native) != 1 {
		return resolution, museReasonNativeAmbiguous, errors.New("muse socket: native entry ambiguous")
	}
	if err := r.authenticateNative(anchor, candidates.native[0]); err != nil {
		return resolution, museReasonNativeUnsafe, err
	}
	if err := r.revalidate(run); err != nil {
		return resolution, r.reason(ctx, err, museReasonNativeUnsafe), err
	}
	return resolution, "", nil
}

func (r museResolver) boundary(ctx context.Context) (museProcessToken, error) {
	if r.pid <= 0 || (r.euid != os.Geteuid() && r.procRoot == "/proc") {
		return museProcessToken{}, errors.New("muse socket: invalid process identity")
	}
	chain := make([]museProcessToken, 0, museParentTransitions+1)
	pid := r.pid
	for range museParentTransitions + 1 {
		if err := ctx.Err(); err != nil {
			return museProcessToken{}, fmt.Errorf("muse socket: lineage context: %w", err)
		}
		token, err := r.readProcess(pid)
		if err != nil {
			return museProcessToken{}, err
		}
		chain = append(chain, token)
		pid = token.parent
	}
	if chain[0].parent != chain[1].pid || chain[1].parent != chain[2].pid ||
		chain[0].pid == chain[1].pid || chain[1].pid == chain[2].pid {
		return museProcessToken{}, errors.New("muse socket: invalid two-hop lineage")
	}
	for _, token := range chain {
		if err := r.revalidate(token); err != nil {
			return museProcessToken{}, err
		}
	}
	return chain[2], nil
}

func (r museResolver) readProcess(pid int) (museProcessToken, error) {
	if pid <= 0 {
		return museProcessToken{}, errors.New("muse socket: invalid pid")
	}
	directory := filepath.Join(r.procRoot, strconv.Itoa(pid))
	var directoryStat unix.Stat_t
	if err := unix.Stat(directory, &directoryStat); err != nil {
		return museProcessToken{}, fmt.Errorf("muse socket: stat process: %w", err)
	}
	if int64(directoryStat.Uid) != int64(r.euid) {
		return museProcessToken{}, errors.New("muse socket: process uid mismatch")
	}
	raw, err := readMuseBoundedFile(filepath.Join(directory, "stat"), museMaxProcStatBytes)
	if err != nil {
		return museProcessToken{}, err
	}
	prefix := strconv.Itoa(pid) + " ("
	closing := strings.LastIndexByte(string(raw), ')')
	if !strings.HasPrefix(string(raw), prefix) || closing < len(prefix) {
		return museProcessToken{}, errors.New("muse socket: malformed process stat")
	}
	fields := strings.Fields(string(raw[closing+1:]))
	if len(fields) < 20 {
		return museProcessToken{}, errors.New("muse socket: short process stat")
	}
	parent, parentErr := strconv.Atoi(fields[1])
	start, startErr := strconv.ParseUint(fields[19], 10, 64)
	if parentErr != nil || startErr != nil || parent <= 0 || start == 0 {
		return museProcessToken{}, errors.New("muse socket: invalid process stat")
	}
	return museProcessToken{pid: pid, parent: parent, start: start}, nil
}

func readMuseBoundedFile(path string, limit int64) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("muse socket: open bounded file: %w", err)
	}
	defer func() { _ = file.Close() }()
	raw, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return nil, fmt.Errorf("muse socket: read bounded file: %w", err)
	}
	if int64(len(raw)) > limit {
		return nil, errors.New("muse socket: bounded file limit")
	}
	return raw, nil
}

func (r museResolver) revalidate(expected museProcessToken) error {
	actual, err := r.readProcess(expected.pid)
	if err != nil {
		return err
	}
	if actual.parent != expected.parent || actual.start != expected.start {
		return errors.New("muse socket: process identity changed")
	}
	return nil
}

func (r museResolver) socketInodes(
	ctx context.Context,
	run museProcessToken,
) (map[uint64]struct{}, error) {
	if err := r.revalidate(run); err != nil {
		return nil, err
	}
	directory, err := os.Open(filepath.Join(r.procRoot, strconv.Itoa(run.pid), "fd"))
	if err != nil {
		return nil, fmt.Errorf("muse socket: open process fds: %w", err)
	}
	defer func() { _ = directory.Close() }()
	entries, err := directory.ReadDir(museMaxFDEntries + 1)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("muse socket: read process fds: %w", err)
	}
	if len(entries) > museMaxFDEntries {
		return nil, errors.New("muse socket: fd limit")
	}
	inodes := make(map[uint64]struct{})
	buffer := make([]byte, museMaxFDLinkBytes)
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("muse socket: fd context: %w", err)
		}
		if _, err := strconv.Atoi(entry.Name()); err != nil {
			continue
		}
		length, err := unix.Readlinkat(int(directory.Fd()), entry.Name(), buffer)
		if errors.Is(err, unix.ENOENT) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("muse socket: read fd link: %w", err)
		}
		if length == len(buffer) {
			continue
		}
		inode, ok := museSocketInode(string(buffer[:length]))
		if ok {
			inodes[inode] = struct{}{}
		}
	}
	if len(inodes) == 0 {
		return nil, errMuseNoSocketFDs
	}
	if err := r.revalidate(run); err != nil {
		return nil, err
	}
	return inodes, nil
}

// parentHasMuseTopology detects an outer measured run without treating any
// of its sockets as candidates for the inner boundary.
func (r museResolver) parentHasMuseTopology(
	ctx context.Context,
	run museProcessToken,
) (bool, error) {
	parent, err := r.readProcess(run.parent)
	if err != nil {
		return false, err
	}
	inodes, err := r.socketInodes(ctx, parent)
	if errors.Is(err, errMuseNoSocketFDs) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	candidates, err := r.socketCandidates(ctx, inodes)
	if err != nil {
		return false, err
	}
	return len(candidates.agents) != 0 || len(candidates.native) != 0, nil
}

func museSocketInode(target string) (uint64, bool) {
	if !strings.HasPrefix(target, "socket:[") || !strings.HasSuffix(target, "]") {
		return 0, false
	}
	inode, err := strconv.ParseUint(target[len("socket:["):len(target)-1], 10, 64)
	return inode, err == nil && inode > 0
}

func (r museResolver) socketCandidates(
	ctx context.Context,
	inodes map[uint64]struct{},
) (museSocketCandidates, error) {
	file, err := os.Open(filepath.Join(r.procRoot, "net", "unix"))
	if err != nil {
		return museSocketCandidates{}, fmt.Errorf("muse socket: open proc unix: %w", err)
	}
	defer func() { _ = file.Close() }()
	limited := &io.LimitedReader{R: file, N: museMaxProcUnixBytes + 1}
	reader := bufio.NewReader(limited)
	remaining := make(map[uint64]struct{}, len(inodes))
	for inode := range inodes {
		remaining[inode] = struct{}{}
	}
	var candidates museSocketCandidates
	first := true
	for {
		if err := ctx.Err(); err != nil {
			return museSocketCandidates{}, fmt.Errorf("muse socket: unix table context: %w", err)
		}
		line, readErr := reader.ReadString('\n')
		consumed := museMaxProcUnixBytes + 1 - limited.N
		if consumed > museMaxProcUnixBytes {
			return museSocketCandidates{}, errors.New("muse socket: proc unix limit")
		}
		if readErr != nil {
			if !errors.Is(readErr, io.EOF) {
				return museSocketCandidates{}, fmt.Errorf("muse socket: read proc unix: %w", readErr)
			}
			if len(line) != 0 {
				return museSocketCandidates{}, errors.New("muse socket: incomplete proc unix record")
			}
		}
		if !first && strings.TrimSpace(line) != "" {
			if err := collectMuseSocketCandidate(line, inodes, remaining, &candidates); err != nil {
				return museSocketCandidates{}, err
			}
		}
		first = false
		if len(remaining) == 0 {
			break
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
	}
	return candidates, nil
}

func collectMuseSocketCandidate(
	line string,
	inodes map[uint64]struct{},
	remaining map[uint64]struct{},
	candidates *museSocketCandidates,
) error {
	candidate, inode, listen, err := museParseUnixLine(line)
	if err != nil {
		return err
	}
	if _, wanted := inodes[inode]; !wanted {
		return nil
	}
	delete(remaining, inode)
	if !listen {
		return nil
	}
	switch candidateFamily(candidate.path) {
	case museAgentsDirectory:
		candidates.agents = append(candidates.agents, candidate)
	case museNativeDirectory:
		candidates.native = append(candidates.native, candidate)
	}
	return nil
}

func museParseUnixLine(line string) (museSocketCandidate, uint64, bool, error) {
	fields := strings.Fields(line)
	if len(fields) < 7 {
		return museSocketCandidate{}, 0, false, errors.New("muse socket: malformed proc unix")
	}
	flags, flagsErr := strconv.ParseUint(fields[3], 16, 32)
	typeValue, typeErr := strconv.ParseUint(fields[4], 16, 16)
	state, stateErr := strconv.ParseUint(fields[5], 16, 8)
	inode, inodeErr := strconv.ParseUint(fields[6], 10, 64)
	if flagsErr != nil || typeErr != nil || stateErr != nil || inodeErr != nil || inode == 0 {
		return museSocketCandidate{}, 0, false, errors.New("muse socket: invalid proc unix")
	}
	listen := flags&museListenFlag != 0 && typeValue == museSocketStream &&
		state == museSocketUnconnected && len(fields) >= 8
	if !listen {
		return museSocketCandidate{}, inode, false, nil
	}
	path := strings.Join(fields[7:], " ")
	return museSocketCandidate{path: path}, inode, true, nil
}

func candidateFamily(path string) string {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return ""
	}
	return filepath.Base(filepath.Dir(path))
}

func (r museResolver) authenticateAgent(
	run museProcessToken,
	candidate museSocketCandidate,
) (*museAnchor, error) {
	runtime, agent, runtimeFD, err := openMuseAgentPath(candidate, r.euid)
	if err != nil {
		return nil, err
	}
	runtimeIdentity, err := musePrivateDirectoryIdentity(runtimeFD, r.euid)
	if err != nil {
		_ = unix.Close(runtimeFD)
		return nil, err
	}
	return &museAnchor{
		run: run, runtime: runtime, agent: agent, runtimeFD: runtimeFD,
		runtimeIdentity: runtimeIdentity, nativeSocketFD: -1,
	}, nil
}

func openMuseAgentPath(
	candidate museSocketCandidate,
	euid int,
) (string, protocol.AgentID, int, error) {
	runtime, agent, ok := splitMuseSocketPath(candidate.path, museAgentsDirectory)
	if !ok {
		return "", "", -1, errors.New("muse socket: invalid agent path")
	}
	parts := strings.Split(strings.TrimPrefix(candidate.path, "/"), "/")
	runtimeIndex := len(parts) - 3
	familyIndex := len(parts) - 2
	current, err := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return "", "", -1, fmt.Errorf("muse socket: open root: %w", err)
	}
	runtimeFD := -1
	defer func() { _ = unix.Close(current) }()
	for index := range familyIndex + 1 {
		next, openErr := unix.Openat(
			current, parts[index], unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0,
		)
		if openErr != nil {
			if runtimeFD >= 0 {
				_ = unix.Close(runtimeFD)
			}
			return "", "", -1, fmt.Errorf("muse socket: open path component: %w", openErr)
		}
		_ = unix.Close(current)
		current = next
		if index == runtimeIndex || index == familyIndex {
			if privateErr := validateMusePrivateDirectory(current, euid); privateErr != nil {
				if runtimeFD >= 0 {
					_ = unix.Close(runtimeFD)
				}
				return "", "", -1, privateErr
			}
		}
		if index == runtimeIndex {
			runtimeFD, err = unix.FcntlInt(uintptr(current), unix.F_DUPFD_CLOEXEC, 0)
			if err != nil {
				return "", "", -1, fmt.Errorf("muse socket: retain runtime: %w", err)
			}
		}
	}
	if err := validateMuseSocket(current, parts[len(parts)-1], euid); err != nil {
		_ = unix.Close(runtimeFD)
		return "", "", -1, err
	}
	return runtime, agent, runtimeFD, nil
}

func splitMuseSocketPath(
	path string,
	family string,
) (string, protocol.AgentID, bool) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return "", "", false
	}
	parts := strings.Split(strings.TrimPrefix(path, "/"), "/")
	if len(parts) < 3 || parts[len(parts)-2] != family {
		return "", "", false
	}
	name := parts[len(parts)-1]
	if !strings.HasSuffix(name, ".sock") {
		return "", "", false
	}
	agent := protocol.AgentID(strings.TrimSuffix(name, ".sock"))
	if !agent.Valid() {
		return "", "", false
	}
	runtime := "/" + strings.Join(parts[:len(parts)-2], "/")
	return runtime, agent, true
}

func musePrivateDirectoryIdentity(fd int, euid int) (museFileIdentity, error) {
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return museFileIdentity{}, fmt.Errorf("muse socket: stat private directory: %w", err)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFDIR || int64(stat.Uid) != int64(euid) || stat.Mode&0o777 != 0o700 {
		return museFileIdentity{}, errors.New("muse socket: unsafe private directory")
	}
	return museFileIdentity{device: stat.Dev, inode: stat.Ino}, nil
}

func validateMusePrivateDirectory(fd int, euid int) error {
	_, err := musePrivateDirectoryIdentity(fd, euid)
	return err
}

func museSocketIdentity(fd int, name string, euid int) (museFileIdentity, error) {
	var stat unix.Stat_t
	if err := unix.Fstatat(fd, name, &stat, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return museFileIdentity{}, fmt.Errorf("muse socket: stat socket: %w", err)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFSOCK || int64(stat.Uid) != int64(euid) || stat.Mode&0o777 != 0o600 {
		return museFileIdentity{}, errors.New("muse socket: unsafe socket")
	}
	return museFileIdentity{device: stat.Dev, inode: stat.Ino}, nil
}

func validateMuseSocket(fd int, name string, euid int) error {
	_, err := museSocketIdentity(fd, name, euid)
	return err
}

func (r museResolver) authenticateNative(
	anchor *museAnchor,
	candidate museSocketCandidate,
) error {
	runtime, agent, ok := splitMuseSocketPath(candidate.path, museNativeDirectory)
	if !ok || runtime != anchor.runtime || agent != anchor.agent {
		return errors.New("muse socket: native identity mismatch")
	}
	directory, err := unix.Openat(
		anchor.runtimeFD, museNativeDirectory,
		unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0,
	)
	if err != nil {
		return fmt.Errorf("muse socket: open native directory: %w", err)
	}
	defer func() { _ = unix.Close(directory) }()
	directoryIdentity, err := musePrivateDirectoryIdentity(directory, r.euid)
	if err != nil {
		return err
	}
	socketIdentity, err := museSocketIdentity(directory, string(agent)+".sock", r.euid)
	if err != nil {
		return err
	}
	socketFD, err := unix.Openat(
		directory, string(agent)+".sock", unix.O_PATH|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0,
	)
	if err != nil {
		return fmt.Errorf("muse socket: open native socket: %w", err)
	}
	socketFDIdentity, err := museSocketFDIdentity(socketFD, r.euid)
	if err != nil {
		_ = unix.Close(socketFD)
		return err
	}
	if socketFDIdentity != socketIdentity {
		_ = unix.Close(socketFD)
		return errors.New("muse socket: native socket changed while opening")
	}
	anchor.nativeDirectoryIdentity = directoryIdentity
	anchor.nativeSocketIdentity = socketIdentity
	anchor.nativeSocketFD = socketFD
	return nil
}

// authenticateSystemNative binds the hardened system node entry to an
// anchor whose run discovery found no measured topology. It reuses the
// same candidate socketPath resolves to, opens it without following
// links, and pins its identity. It changes nothing the gifford-run path
// authenticates: that path keeps its own directory, socket and peer
// checks untouched.
func (r museResolver) authenticateSystemNative(anchor *museAnchor) error {
	path := systemNativeSocket()
	if path == "" {
		return errors.New("muse socket: system native entry unavailable")
	}
	socketFD, err := unix.Openat(
		unix.AT_FDCWD, path, unix.O_PATH|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0,
	)
	if err != nil {
		return fmt.Errorf("muse socket: open system native entry: %w", err)
	}
	var stat unix.Stat_t
	if err := unix.Fstat(socketFD, &stat); err != nil {
		_ = unix.Close(socketFD)
		return fmt.Errorf("muse socket: stat system native entry: %w", err)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFSOCK {
		_ = unix.Close(socketFD)
		return errors.New("muse socket: system native entry is not a socket")
	}
	anchor.nativeSocketIdentity = museFileIdentity{device: stat.Dev, inode: stat.Ino}
	anchor.systemPeerUID = stat.Uid
	anchor.nativeSocketFD = socketFD
	anchor.systemNative = true
	return nil
}

func (r museResolver) revalidateNativeRoute(anchor *museAnchor) error {
	err := r.revalidateNativeRouteInner(anchor)
	if err != nil {
		return fmt.Errorf("%w: %w", errMuseNativeRouteChanged, err)
	}
	return nil
}

func (r museResolver) revalidateNativeRouteInner(anchor *museAnchor) error {
	if anchor.systemNative {
		return r.revalidateSystemNativeRoute(anchor)
	}
	currentRuntime, err := openMuseRuntimePath(anchor.runtime, r.euid)
	if err != nil {
		return err
	}
	currentRuntimeIdentity, err := musePrivateDirectoryIdentity(currentRuntime, r.euid)
	_ = unix.Close(currentRuntime)
	if err != nil {
		return err
	}
	if currentRuntimeIdentity != anchor.runtimeIdentity {
		return errors.New("muse socket: runtime path identity changed")
	}
	directory, err := unix.Openat(
		anchor.runtimeFD, museNativeDirectory,
		unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0,
	)
	if err != nil {
		return fmt.Errorf("muse socket: reopen native directory: %w", err)
	}
	defer func() { _ = unix.Close(directory) }()
	directoryIdentity, err := musePrivateDirectoryIdentity(directory, r.euid)
	if err != nil {
		return err
	}
	if directoryIdentity != anchor.nativeDirectoryIdentity {
		return errors.New("muse socket: native directory identity changed")
	}
	socketIdentity, err := museSocketIdentity(directory, string(anchor.agent)+".sock", r.euid)
	if err != nil {
		return err
	}
	if socketIdentity != anchor.nativeSocketIdentity {
		return errors.New("muse socket: native socket identity changed")
	}
	socketFDIdentity, err := museSocketFDIdentity(anchor.nativeSocketFD, r.euid)
	if err != nil {
		return err
	}
	if socketFDIdentity != anchor.nativeSocketIdentity {
		return errors.New("muse socket: open native socket identity changed")
	}
	return nil
}

func museSocketFDIdentity(fd int, euid int) (museFileIdentity, error) {
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return museFileIdentity{}, fmt.Errorf("muse socket: stat open socket: %w", err)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFSOCK || int64(stat.Uid) != int64(euid) || stat.Mode&0o777 != 0o600 {
		return museFileIdentity{}, errors.New("muse socket: unsafe open socket")
	}
	return museFileIdentity{device: stat.Dev, inode: stat.Ino}, nil
}

// revalidateSystemNativeRoute rechecks the retained system socket alone:
// the shared entry is owned by the node, never by the hook user, so only
// its identity must still match. There is no per-run directory to reopen.
func (r museResolver) revalidateSystemNativeRoute(anchor *museAnchor) error {
	var stat unix.Stat_t
	if err := unix.Fstat(anchor.nativeSocketFD, &stat); err != nil {
		return fmt.Errorf("muse socket: stat system native entry: %w", err)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFSOCK ||
		stat.Dev != anchor.nativeSocketIdentity.device ||
		stat.Ino != anchor.nativeSocketIdentity.inode {
		return errors.New("muse socket: system native entry changed")
	}
	return nil
}

func openMuseRuntimePath(path string, euid int) (int, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || path == "/" {
		return -1, errors.New("muse socket: invalid runtime path")
	}
	current, err := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, fmt.Errorf("muse socket: reopen root: %w", err)
	}
	for _, part := range strings.Split(strings.TrimPrefix(path, "/"), "/") {
		next, openErr := unix.Openat(
			current, part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0,
		)
		_ = unix.Close(current)
		if openErr != nil {
			return -1, fmt.Errorf("muse socket: reopen runtime component: %w", openErr)
		}
		current = next
	}
	if err := validateMusePrivateDirectory(current, euid); err != nil {
		_ = unix.Close(current)
		return -1, err
	}
	return current, nil
}

func (r museResolver) authenticatePeer(conn net.Conn, anchor *museAnchor) error {
	credentials, err := musePeerCredentials(conn)
	if err != nil {
		return err
	}
	if anchor.systemNative {
		return r.authenticateSystemPeer(credentials, anchor)
	}
	if credentials == nil || int64(credentials.Uid) != int64(r.euid) || int(credentials.Pid) != anchor.run.pid {
		return errors.New("muse socket: peer credentials mismatch")
	}
	return r.revalidate(anchor.run)
}

// authenticateSystemPeer checks the dialed peer against the pinned system
// socket owner: the listener must run as the uid that owned the entry at
// bind time. The run pid check of the per-run path cannot apply: the node
// server is never a run process.
func (r museResolver) authenticateSystemPeer(credentials *unix.Ucred, anchor *museAnchor) error {
	if credentials == nil || credentials.Uid != anchor.systemPeerUID {
		return errors.New("muse socket: system peer mismatch")
	}
	return r.revalidate(anchor.run)
}

func musePeerCredentials(conn net.Conn) (*unix.Ucred, error) {
	unixConn, ok := conn.(*net.UnixConn)
	if !ok {
		return nil, errors.New("muse socket: peer is not unix")
	}
	raw, err := unixConn.SyscallConn()
	if err != nil {
		return nil, fmt.Errorf("muse socket: access peer: %w", err)
	}
	var credentials *unix.Ucred
	var credentialsErr error
	if err := raw.Control(func(fd uintptr) {
		credentials, credentialsErr = unix.GetsockoptUcred(
			int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED,
		)
	}); err != nil {
		return nil, fmt.Errorf("muse socket: read peer credentials: %w", err)
	}
	if credentialsErr != nil {
		return nil, fmt.Errorf("muse socket: get peer credentials: %w", credentialsErr)
	}
	return credentials, nil
}

func (r museResolver) mark(
	ctx context.Context,
	anchor *museAnchor,
	reason museMarkerReason,
) {
	if anchor == nil || reason == "" || ctx.Err() != nil || !validMuseMarkerReason(reason) {
		return
	}
	root, err := openMuseMarkerDirectory(anchor.runtimeFD, museDiagnosticDirectory, r.euid)
	if err != nil {
		return
	}
	defer func() { _ = unix.Close(root) }()
	if ctx.Err() != nil {
		return
	}
	directory, err := openMuseMarkerDirectory(root, museDiagnosticMuseFolder, r.euid)
	if err != nil {
		return
	}
	defer func() { _ = unix.Close(directory) }()
	if ctx.Err() != nil {
		return
	}
	tokenDirectory, err := openMuseMarkerDirectory(directory, museMarkerToken(anchor.run), r.euid)
	if err != nil {
		return
	}
	defer func() { _ = unix.Close(tokenDirectory) }()
	if ctx.Err() != nil {
		return
	}
	name := string(reason) + ".marker"
	fd, err := unix.Openat(
		tokenDirectory, name,
		unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC,
		0o600,
	)
	if errors.Is(err, unix.EEXIST) {
		return
	}
	if err != nil {
		return
	}
	_ = unix.Fchmod(fd, 0o600)
	_ = unix.Close(fd)
}

func validMuseMarkerReason(reason museMarkerReason) bool {
	if len(museMarkerReasons) > museMarkerReasonLimit {
		return false
	}
	for _, known := range museMarkerReasons {
		if reason == known {
			return true
		}
	}
	return false
}

func openMuseMarkerDirectory(parent int, name string, euid int) (int, error) {
	mkdirErr := unix.Mkdirat(parent, name, 0o700)
	if mkdirErr != nil && !errors.Is(mkdirErr, unix.EEXIST) {
		return -1, fmt.Errorf("muse socket: create marker directory: %w", mkdirErr)
	}
	directory, err := unix.Openat(
		parent, name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0,
	)
	if err != nil {
		return -1, fmt.Errorf("muse socket: open marker directory: %w", err)
	}
	if mkdirErr == nil {
		if err := unix.Fchmod(directory, 0o700); err != nil {
			_ = unix.Close(directory)
			return -1, fmt.Errorf("muse socket: set marker directory mode: %w", err)
		}
	}
	if err := validateMusePrivateDirectory(directory, euid); err != nil {
		_ = unix.Close(directory)
		return -1, err
	}
	return directory, nil
}

func museMarkerToken(run museProcessToken) string {
	digest := sha256.Sum256([]byte(strconv.Itoa(run.pid) + ":" + strconv.FormatUint(run.start, 10)))
	return hex.EncodeToString(digest[:16])
}
