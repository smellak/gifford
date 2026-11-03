//go:build linux

package writerlog

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"time"

	"golang.org/x/sys/unix"
)

func exchange(
	ctx context.Context,
	socketPath string,
	lockFile *os.File,
	body []byte,
) ([]byte, error) {
	connection, err := (&net.Dialer{}).DialContext(ctx, "unixpacket", socketPath)
	if err != nil {
		return nil, transportError(ctx, err)
	}
	defer func() { _ = connection.Close() }()
	unixConnection, ok := connection.(*net.UnixConn)
	if !ok {
		return nil, errors.New("unexpected local socket type")
	}
	if deadline, ok := ctx.Deadline(); ok {
		if err := unixConnection.SetDeadline(deadline); err != nil {
			return nil, transportError(ctx, err)
		}
	}
	stop := context.AfterFunc(ctx, func() {
		_ = unixConnection.SetDeadline(time.Unix(1, 0))
	})
	defer stop()

	rights := unix.UnixRights(int(lockFile.Fd()))
	written, controlWritten, err := unixConnection.WriteMsgUnix(body, rights, nil)
	if err != nil {
		return nil, transportError(ctx, err)
	}
	if written != len(body) || controlWritten != len(rights) {
		return nil, errors.New("short local socket write")
	}
	ack := make([]byte, maxACKLen+1)
	read, controlRead, flags, _, err := unixConnection.ReadMsgUnix(ack, nil)
	if err != nil {
		return nil, transportError(ctx, err)
	}
	if read == 0 || controlRead != 0 || flags&(unix.MSG_TRUNC|unix.MSG_CTRUNC) != 0 {
		return nil, errors.New("invalid local socket reply")
	}
	return ack[:read], nil
}

func transportError(ctx context.Context, err error) error {
	if contextErr := ctx.Err(); contextErr != nil {
		return fmt.Errorf("local socket context: %w", contextErr)
	}
	return err
}
