//go:build conformance && linux

package conformance

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	statusPass        = "PASS"
	statusFail        = "FAIL"
	statusNotExecuted = "NOT_EXECUTED"

	defaultReadinessWindow  = time.Second
	defaultReadinessTimeout = 60 * time.Second
	defaultReadinessPoll    = 50 * time.Millisecond
)

type readinessPolicy struct {
	window  time.Duration
	timeout time.Duration
	poll    time.Duration
}

var conformanceReadiness = readinessPolicy{
	window:  defaultReadinessWindow,
	timeout: defaultReadinessTimeout,
	poll:    defaultReadinessPoll,
}

type readinessObservation struct {
	draftState      string
	activityState   string
	guardAvailable  bool
	processes       []processInfo
	processesExact  bool
	outputBytes     int64
	outputAvailable bool
	outputQuietFor  time.Duration
	stableFor       time.Duration
}

type readinessError struct {
	evidence string
}

func (err *readinessError) Error() string { return err.evidence }

func scenarioErrorStatus(err error) string {
	var readinessErr *readinessError
	if errors.As(err, &readinessErr) {
		return statusNotExecuted
	}
	return statusFail
}

func exactHarnessProcessTree(root int) ([]processInfo, bool) {
	processes := descendants(root)
	return processes, len(processes) == 1 && processes[0].ppid == root
}

func (run *conformanceRun) readinessObservation() readinessObservation {
	var observation readinessObservation
	guardErr := run.database.QueryRowContext(run.ctx, `SELECT draft_state, activity_state
		FROM input_guards WHERE session_id = ? AND agent_id = ?`,
		run.activeSession, conformanceRecipient).
		Scan(&observation.draftState, &observation.activityState)
	observation.guardAvailable = guardErr == nil
	observation.processes, observation.processesExact = exactHarnessProcessTree(run.tmux.panePID)
	outputBytes, outputErr := run.tmux.outputSize()
	observation.outputBytes = outputBytes
	observation.outputAvailable = outputErr == nil
	return observation
}

func (observation readinessObservation) guardReady() bool {
	return observation.guardAvailable && observation.draftState == draftClosed &&
		observation.activityState == "IDLE"
}

func (observation readinessObservation) instantReady() bool {
	return observation.guardReady() && observation.processesExact && observation.outputAvailable
}

func (observation readinessObservation) evidence(window time.Duration) string {
	guard := "unavailable"
	if observation.guardAvailable {
		guard = observation.draftState + "/" + observation.activityState
	}
	children := processTreeEvidence(observation.processes)
	if children == "" {
		children = "no descendants"
	}
	output := "unavailable"
	if observation.outputAvailable {
		output = fmt.Sprintf("bytes=%d quiet_for=%s", observation.outputBytes,
			readinessDuration(observation.outputQuietFor))
	}
	return fmt.Sprintf("guard=%s; harness_processes=%d exact=%t children=[%s]; output=%s; stable_window=%s/%s",
		guard, len(observation.processes), observation.processesExact, children, output,
		readinessDuration(observation.stableFor), readinessDuration(window))
}

func readinessDuration(duration time.Duration) string {
	if duration < 0 {
		duration = 0
	}
	return duration.Truncate(10 * time.Millisecond).String()
}

func (run *conformanceRun) waitForReadiness(policy readinessPolicy) (string, error) {
	if policy.window <= 0 || policy.timeout <= 0 || policy.poll <= 0 {
		return "", errors.New("invalid readiness policy")
	}
	started := time.Now()
	timer := time.NewTimer(policy.timeout)
	defer timer.Stop()
	ticker := time.NewTicker(policy.poll)
	defer ticker.Stop()

	var (
		candidateSince  time.Time
		lastOutputBytes int64
		lastOutputAt    = started
		outputSeen      bool
		lastObservation readinessObservation
	)
	for {
		now := time.Now()
		observation := run.readinessObservation()
		outputChanged := false
		if observation.outputAvailable {
			outputChanged = !outputSeen || observation.outputBytes != lastOutputBytes
			if outputChanged {
				lastOutputAt = now
				lastOutputBytes = observation.outputBytes
				outputSeen = true
			}
		} else {
			outputSeen = false
			lastOutputAt = now
		}

		if observation.instantReady() {
			if candidateSince.IsZero() || outputChanged {
				candidateSince = now
			}
		} else {
			candidateSince = time.Time{}
		}
		if outputSeen {
			observation.outputQuietFor = now.Sub(lastOutputAt)
		}
		if !candidateSince.IsZero() {
			observation.stableFor = now.Sub(candidateSince)
		}
		lastObservation = observation
		if observation.instantReady() && observation.stableFor >= policy.window {
			return "readiness=yes; " + observation.evidence(policy.window), nil
		}

		select {
		case <-run.ctx.Done():
			return "", &readinessError{evidence: "readiness interrupted: " +
				lastObservation.evidence(policy.window)}
		case <-timer.C:
			return "", &readinessError{evidence: fmt.Sprintf("readiness timeout after %s: %s",
				readinessDuration(time.Since(started)), lastObservation.evidence(policy.window))}
		case <-ticker.C:
		}
	}
}

func (run *conformanceRun) waitForScenarioReadiness() error {
	evidence, err := run.waitForReadiness(conformanceReadiness)
	if err == nil {
		run.readinessEvidence = strings.TrimPrefix(evidence, "readiness=yes; ")
	}
	return err
}

func (run *conformanceRun) prepareOpenCodeSession(
	policy readinessPolicy,
) (evidence, readiness string, err error) {
	run.prepared = false
	input := append([]byte(preparePrompt), []byte(run.config.profile.Submit)...)
	if err := run.tmux.sendBytes(run.ctx, input); err != nil {
		return "mode=opencode-ok; prompt_sent=no; submit=profile; readiness=no", "", err
	}
	readiness, err = run.waitForReadiness(policy)
	if err != nil {
		return "mode=opencode-ok; prompt_sent=yes; submit=profile; readiness=no; " + err.Error(), "", err
	}
	run.prepared = true
	return "mode=opencode-ok; prompt_sent=yes; submit=profile; " + readiness, readiness, nil
}

func (run *conformanceRun) scenarioPrepare() (string, error) {
	evidence, _, err := run.prepareOpenCodeSession(conformanceReadiness)
	if err != nil {
		return "", errors.New(evidence)
	}
	return evidence, nil
}

func (run *conformanceRun) prepareReplacementSession() error {
	evidence, readiness, prepareErr := run.prepareOpenCodeSession(conformanceReadiness)
	status := statusPass
	root := ""
	if prepareErr != nil {
		status = statusFail
		root = run.config.root
	}
	if err := run.config.report.Record(run.config.harness, run.config.version,
		scenarioS5Prepare, status, evidence, root); err != nil {
		return err
	}
	if prepareErr != nil {
		return &readinessError{evidence: "S5 replacement preparation failed: " + evidence}
	}
	run.readinessEvidence = strings.TrimPrefix(readiness, "readiness=yes; ")
	return nil
}
