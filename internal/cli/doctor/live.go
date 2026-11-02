package doctor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/exitcode"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/tui"
	"github.com/smellak/gifford/internal/xdg"
)

const liveTimeout = 2 * time.Second

// Check states. Only stateFail makes the command fail; SKIP and INFO report
// what was not verified.
const (
	stateOK   = "OK"
	stateFail = "FAIL"
	stateSkip = "SKIP"
	stateInfo = "INFO"
)

// Only metadata reads are made: doctor neither consumes a message nor wakes an
// agent, starts a model, changes a channel control or captures a terminal.
type metadataReader interface {
	Call(context.Context, protocol.ControlMethod, any, any) error
}

type liveCheck struct {
	name, state, detail string
}

func runLive(ctx context.Context, out io.Writer, runtimeDir, service, peer string) error {
	if runtimeDir == "" {
		dirs, err := xdg.Resolve()
		if err != nil {
			return fmt.Errorf("resolve doctor runtime directory: %w", err)
		}
		runtimeDir = dirs.Runtime
	}
	if !filepath.IsAbs(runtimeDir) {
		return &deps.ExitError{Code: exitcode.Usage, Err: errors.New("--runtime-dir must be absolute")}
	}
	client, err := tui.NewClient(tui.ClientOptions{
		SocketPath: filepath.Join(runtimeDir, "daemon.sock"), Timeout: liveTimeout,
	})
	if err != nil {
		return fmt.Errorf("create doctor control client: %w", err)
	}

	checks := make([]liveCheck, 0, 8)
	if service != "" {
		serviceCtx, cancel := context.WithTimeout(ctx, liveTimeout)
		err := exec.CommandContext(serviceCtx, "systemctl", "is-active", "--quiet", "--", service).Run()
		cancel()
		check := liveCheck{"SERVICE", stateOK, "active: " + service}
		if err != nil {
			check.state, check.detail = stateFail, "not active or inaccessible: "+service
		}
		checks = append(checks, check)
	}
	checks = append(checks, checkListener(ctx, filepath.Join(runtimeDir, "hub.sock")))
	checks = append(checks, checkMetadata(ctx, client)...)
	// Registration is not proof that the service can access the real tmux pane.
	// The machine-node integration must supply that evidence separately.
	checks = append(checks, liveCheck{"TMUX", stateSkip, "per-agent access not probed; requires the machine-node integration"})
	if peer != "" {
		checks = append(checks, liveCheck{"FEDERATION", stateFail, "requested peer check is not implemented"})
	} else {
		checks = append(checks, liveCheck{"FEDERATION", stateSkip, "no end-to-end peer check performed"})
	}
	return writeLiveReport(out, checks)
}

func checkListener(ctx context.Context, socket string) liveCheck {
	ctx, cancel := context.WithTimeout(ctx, liveTimeout)
	defer cancel()
	connection, err := (&net.Dialer{}).DialContext(ctx, "unix", socket)
	if err != nil {
		return liveCheck{"HUB.SOCK", stateFail, "no accessible listener"}
	}
	_ = connection.Close()
	return liveCheck{"HUB.SOCK", stateOK, "listener accepts connections; agent authentication not probed"}
}

func checkMetadata(ctx context.Context, client metadataReader) []liveCheck {
	checks := make([]liveCheck, 0, 3)
	var channel protocol.ChannelState
	if err := client.Call(ctx, protocol.ControlMethodChannelGetGlobal, protocol.EmptyParams{}, &channel); err != nil {
		checks = append(checks, liveCheck{"CONTROL", stateFail, "channel query unavailable"})
	} else {
		checks = append(checks, liveCheck{"CONTROL", stateOK, fmt.Sprintf("responding; channel %s (effective %s)", channel.Mode, channel.EffectiveMode)})
	}
	var agents protocol.AgentListResult
	if err := client.Call(ctx, protocol.ControlMethodAgentList, protocol.AgentListParams{}, &agents); err != nil {
		checks = append(checks, liveCheck{"AGENTS", stateFail, "agent registry query unavailable"})
	} else {
		detail := fmt.Sprintf("%d registered agents; this is not a delivery test", len(agents.Agents))
		if agents.NextCursor != "" {
			detail += " (first page only)"
		}
		checks = append(checks, liveCheck{"AGENTS", stateInfo, detail})
	}
	var model protocol.ModelStatusResult
	if err := client.Call(ctx, protocol.ControlMethodModelStatus, protocol.EmptyParams{}, &model); err != nil {
		checks = append(checks, liveCheck{"GLIMMER", stateFail, "model status query unavailable"})
	} else {
		detail := "no recorded call; current Glimmer/fallback availability is not verified"
		if model.LastCall != nil {
			detail = fmt.Sprintf("last call %s at %s (%d ms); not a live availability probe",
				model.LastCall.Status, model.LastCall.ObservedAt.UTC().Format(time.RFC3339), model.LastCall.LatencyMS)
		}
		checks = append(checks, liveCheck{"GLIMMER", stateInfo, detail})
	}
	return checks
}

func writeLiveReport(out io.Writer, checks []liveCheck) error {
	rep := &reportWriter{w: out}
	ok := true
	for _, check := range checks {
		rep.printf("%-11s %-4s %s\n", check.name, check.state, check.detail)
		if check.state == stateFail {
			ok = false
		}
	}
	rep.printf("Live diagnostics only: INFO and SKIP are not verified delivery or model checks.\n")
	if rep.err != nil {
		return fmt.Errorf("write live doctor report: %w", rep.err)
	}
	if !ok {
		return &deps.ExitError{Code: exitcode.Error, Err: errors.New("live doctor found problems or a requested check is unavailable")}
	}
	return nil
}
