package agent

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"time"
)

// evosciBlockedArgs are flags the daemon sets itself; user-supplied custom_args
// must not override them.
var evosciBlockedArgs = map[string]blockedArgMode{
	"-p":              blockedWithValue,  // the task prompt
	"--prompt":        blockedWithValue,  // alias of -p
	"--output-format": blockedWithValue,  // stream-json protocol for daemon parsing
	"--auto-mode":     blockedStandalone, // unattended: no approval / ask_user prompts
	"--dangerous":     blockedStandalone, // real-filesystem access; daemon sets it for fleet parity
	"--workdir":       blockedWithValue,  // task workdir anchor
	"--use-cwd":       blockedStandalone, // conflicts with --workdir
	"--mode":          blockedWithValue,  // conflicts with --workdir
	"--ui":            blockedWithValue,  // force headless (no TUI/webui)
}

// evosciBackend implements Backend by spawning
// `EvoSci -p <prompt> --output-format stream-json --auto-mode --dangerous --workdir <cwd>`
// and reading EvoScientist's native flat JSONL event stream from stdout — one
// self-describing JSON object per line. See the EvoScientist docs/stream-json.md
// contract for the event schema.
//
// --dangerous disables EvoScientist's workspace confinement so it operates on
// the real filesystem, matching every other Multica backend (claude, codex,
// copilot, opencode, cursor, … all run unconfined + auto-approve headlessly).
// Without it EvoScientist uniquely remaps absolute paths under --workdir (strips
// the leading "/"), so files the agent "saves" to an absolute path silently land
// inside the throwaway task workdir instead of the real target.
type evosciBackend struct {
	cfg Config
}

func (b *evosciBackend) Execute(ctx context.Context, prompt string, opts ExecOptions) (*Session, error) {
	execPath := b.cfg.ExecutablePath
	if execPath == "" {
		execPath = "EvoSci"
	}
	resolved, err := exec.LookPath(execPath)
	if err != nil {
		return nil, fmt.Errorf("EvoSci executable not found at %q: %w", execPath, err)
	}
	execPath = resolved

	timeout := opts.Timeout
	runCtx, cancel := runContext(ctx, timeout)

	// --dangerous drops EvoScientist's workspace confinement so absolute paths
	// hit the real filesystem like the rest of the fleet (see the type doc).
	// It implies auto-approve, which --auto-mode already sets.
	args := []string{"-p", prompt, "--output-format", "stream-json", "--auto-mode", "--dangerous"}
	// Anchor EvoScientist's per-task workspace at the daemon's task workdir.
	// --workdir is mutually exclusive with --mode/--use-cwd, which is why those
	// are blocked above.
	if opts.Cwd != "" {
		args = append(args, "--workdir", opts.Cwd)
	}
	// EvoScientist selects its model from its own configuration (EvoSci onboard);
	// there is no per-invocation --model flag, so opts.Model is not forwarded.
	if opts.Model != "" {
		b.cfg.Logger.Debug("evosci ignores per-task model; EvoScientist uses its own configured model", "model", opts.Model)
	}
	if opts.ResumeSessionID != "" {
		args = append(args, "--resume", opts.ResumeSessionID)
	}
	args = append(args, filterCustomArgs(opts.CustomArgs, evosciBlockedArgs, b.cfg.Logger)...)

	cmd := exec.CommandContext(runCtx, execPath, args...)
	hideAgentWindow(cmd)
	b.cfg.Logger.Info("agent command", "exec", execPath, "args", args)
	cmd.WaitDelay = 10 * time.Second
	if opts.Cwd != "" {
		cmd.Dir = opts.Cwd
	}
	cmd.Env = buildEnv(b.cfg.Env)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("evosci stdout pipe: %w", err)
	}
	cmd.Stderr = newLogWriter(b.cfg.Logger, "[evosci:stderr] ")

	if err := cmd.Start(); err != nil {
		cancel()
		return nil, fmt.Errorf("start evosci: %w", err)
	}

	b.cfg.Logger.Info("evosci started", "pid", cmd.Process.Pid, "cwd", opts.Cwd)

	msgCh := make(chan Message, 256)
	resCh := make(chan Result, 1)

	// Close stdout when the context is cancelled so the scanner unblocks.
	go func() {
		<-runCtx.Done()
		_ = stdout.Close()
	}()

	go func() {
		defer cancel()
		defer close(msgCh)
		defer close(resCh)

		startTime := time.Now()
		scanResult := b.processEvents(stdout, msgCh)

		exitErr := cmd.Wait()
		duration := time.Since(startTime)

		if runCtx.Err() == context.DeadlineExceeded {
			scanResult.status = "timeout"
			scanResult.errMsg = fmt.Sprintf("evosci timed out after %s", timeout)
		} else if runCtx.Err() == context.Canceled {
			scanResult.status = "aborted"
			scanResult.errMsg = "execution cancelled"
		} else if exitErr != nil && scanResult.status == "completed" {
			scanResult.status = "failed"
			scanResult.errMsg = fmt.Sprintf("evosci exited with error: %v", exitErr)
		}

		b.cfg.Logger.Info("evosci finished", "pid", cmd.Process.Pid, "status", scanResult.status, "duration", duration.Round(time.Millisecond).String())

		var usage map[string]TokenUsage
		u := scanResult.usage
		if u.InputTokens > 0 || u.OutputTokens > 0 {
			model := opts.Model
			if model == "" {
				model = "evoscientist"
			}
			usage = map[string]TokenUsage{model: u}
		}

		resCh <- Result{
			Status:     scanResult.status,
			Output:     scanResult.output,
			Error:      scanResult.errMsg,
			DurationMs: duration.Milliseconds(),
			Usage:      usage,
		}
	}()

	return &Session{Messages: msgCh, Result: resCh}, nil
}

// evosciResult holds the accumulated state from processing the event stream.
type evosciResult struct {
	status string
	errMsg string
	output string
	usage  TokenUsage
}

// processEvents reads native EvoScientist JSONL events from r, dispatches each
// to ch as a unified Message, and returns the accumulated result. Extracted for
// testability — this is the core scanner loop.
func (b *evosciBackend) processEvents(r io.Reader, ch chan<- Message) evosciResult {
	var output strings.Builder
	var finalOutput string
	var usage TokenUsage
	finalStatus := "completed"
	var finalError string

	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 1024*1024), 10*1024*1024)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		var ev evosciEvent
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			// Stray non-JSON stdout (e.g. a leaked banner) — skip, don't abort.
			continue
		}

		switch ev.Type {
		case "thinking":
			if ev.Content != "" {
				trySend(ch, Message{Type: MessageThinking, Content: ev.Content})
			}
		case "text":
			if ev.Content != "" {
				output.WriteString(ev.Content)
				trySend(ch, Message{Type: MessageText, Content: ev.Content})
			}
		case "subagent_text":
			if ev.Content != "" {
				trySend(ch, Message{Type: MessageText, Content: ev.Content})
			}
		case "tool_call", "subagent_tool_call":
			trySend(ch, Message{
				Type:   MessageToolUse,
				Tool:   ev.Name,
				CallID: string(ev.ID),
				Input:  ev.Args,
			})
		case "tool_result", "subagent_tool_result":
			trySend(ch, Message{
				Type:   MessageToolResult,
				Tool:   ev.Name,
				CallID: string(ev.ID),
				Output: ev.Content,
			})
		case "usage_stats":
			usage.InputTokens += ev.InputTokens
			usage.OutputTokens += ev.OutputTokens
		case "error":
			msg := ev.Message
			if msg == "" {
				msg = "unknown evoscientist error"
			}
			b.cfg.Logger.Warn("evosci error event", "error", msg)
			trySend(ch, Message{Type: MessageError, Content: msg})
			finalStatus = "failed"
			finalError = msg
		case "interrupt", "ask_user":
			// Headless daemon mode cannot satisfy human-in-the-loop requests.
			// With --auto-mode these should not occur; if one does, surface it
			// and fail rather than hang.
			msg := "agent requested human input (unsupported in headless mode)"
			b.cfg.Logger.Warn("evosci HITL event in headless mode", "event", ev.Type)
			trySend(ch, Message{Type: MessageError, Content: msg})
			if finalStatus != "failed" {
				finalStatus = "failed"
				finalError = msg
			}
		case "done":
			// done.response is the authoritative final text.
			if ev.Response != "" {
				finalOutput = ev.Response
			}
		}
		// subagent_start / subagent_end / tool_selection / summarization* are
		// progress-only and intentionally not surfaced as messages.
	}

	if scanErr := scanner.Err(); scanErr != nil {
		b.cfg.Logger.Warn("evosci stdout scanner error", "error", scanErr)
		if finalStatus == "completed" {
			finalStatus = "failed"
			finalError = fmt.Sprintf("stdout read error: %v", scanErr)
		}
	}

	if finalOutput == "" {
		finalOutput = output.String()
	}

	return evosciResult{
		status: finalStatus,
		errMsg: finalError,
		output: finalOutput,
		usage:  usage,
	}
}

// evosciEvent is a single line from EvoScientist's `--output-format stream-json`
// stdout. Every event is a flat object with a `type` discriminator; only the
// fields the daemon consumes are modelled here (unknown fields are ignored, and
// unknown event types are skipped by the switch above).
type evosciEvent struct {
	Type     string         `json:"type"`
	Content  string         `json:"content"`  // text / thinking / tool_result / subagent_text
	Name     string         `json:"name"`     // tool_call / tool_result / subagent_*
	Args     map[string]any `json:"args"`     // tool_call / subagent_tool_call
	ID       flexString     `json:"id"`       // tool_call (string) / thinking (int) — polymorphic
	Message  string         `json:"message"`  // error
	Response string         `json:"response"` // done

	InputTokens  int64 `json:"input_tokens"`  // usage_stats
	OutputTokens int64 `json:"output_tokens"` // usage_stats
}

// flexString decodes a JSON value that EvoScientist emits as either a string
// (tool call ids like "call_1") or a number (thinking ids like 0) into a Go
// string, so one polymorphic field never aborts the whole line's unmarshal.
type flexString string

func (f *flexString) UnmarshalJSON(b []byte) error {
	b = bytes.TrimSpace(b)
	if len(b) == 0 || string(b) == "null" {
		*f = ""
		return nil
	}
	if b[0] == '"' {
		var s string
		if err := json.Unmarshal(b, &s); err != nil {
			return err
		}
		*f = flexString(s)
		return nil
	}
	// Non-string scalar (number, bool): keep its raw literal form.
	*f = flexString(b)
	return nil
}
