package agent

import (
	"log/slog"
	"strings"
	"testing"
)

func TestNewReturnsEvosciBackend(t *testing.T) {
	t.Parallel()

	b, err := New("evosci", Config{})
	if err != nil {
		t.Fatalf("New(evosci): %v", err)
	}
	if _, ok := b.(*evosciBackend); !ok {
		t.Fatalf("expected *evosciBackend, got %T", b)
	}
}

// ── Integration-level tests: processEvents ──
//
// Feed native EvoScientist stream-json lines (one flat JSON object per line)
// through processEvents and verify the accumulated result + emitted messages.

func TestEvosciProcessEventsHappyPath(t *testing.T) {
	t.Parallel()

	b := &evosciBackend{cfg: Config{Logger: slog.Default()}}
	ch := make(chan Message, 256)

	lines := strings.Join([]string{
		`{"type":"thinking","content":"I should write the file.","id":0}`,
		`{"type":"text","content":"Working on it."}`,
		`{"type":"tool_call","name":"write_file","args":{"path":"a.md"},"id":"call_1"}`,
		`{"type":"tool_result","name":"write_file","content":"wrote 3 bytes","success":true}`,
		`{"type":"usage_stats","input_tokens":1200,"output_tokens":340}`,
		`{"type":"done","content":"Done.","response":"Done."}`,
	}, "\n")

	result := b.processEvents(strings.NewReader(lines), ch)

	if result.status != "completed" {
		t.Errorf("status: got %q, want %q", result.status, "completed")
	}
	if result.output != "Done." {
		t.Errorf("output: got %q, want %q (done.response is authoritative)", result.output, "Done.")
	}
	if result.errMsg != "" {
		t.Errorf("errMsg: got %q, want empty", result.errMsg)
	}
	if result.usage.InputTokens != 1200 || result.usage.OutputTokens != 340 {
		t.Errorf("usage: got %+v, want input=1200 output=340", result.usage)
	}

	close(ch)
	var msgs []Message
	for m := range ch {
		msgs = append(msgs, m)
	}
	// thinking, text, tool-use, tool-result = 4 messages
	if len(msgs) != 4 {
		t.Fatalf("expected 4 messages, got %d: %+v", len(msgs), msgs)
	}
	if msgs[0].Type != MessageThinking || msgs[0].Content != "I should write the file." {
		t.Errorf("msg[0]: got %+v, want thinking", msgs[0])
	}
	if msgs[1].Type != MessageText || msgs[1].Content != "Working on it." {
		t.Errorf("msg[1]: got %+v, want text", msgs[1])
	}
	if msgs[2].Type != MessageToolUse || msgs[2].Tool != "write_file" || msgs[2].CallID != "call_1" {
		t.Errorf("msg[2]: got %+v, want tool-use(write_file)", msgs[2])
	}
	if msgs[2].Input["path"] != "a.md" {
		t.Errorf("msg[2].Input: got %+v, want path=a.md", msgs[2].Input)
	}
	if msgs[3].Type != MessageToolResult || msgs[3].Tool != "write_file" || msgs[3].Output != "wrote 3 bytes" {
		t.Errorf("msg[3]: got %+v, want tool-result", msgs[3])
	}
}

func TestEvosciProcessEventsErrorCausesFailedStatus(t *testing.T) {
	t.Parallel()

	b := &evosciBackend{cfg: Config{Logger: slog.Default()}}
	ch := make(chan Message, 256)

	lines := strings.Join([]string{
		`{"type":"text","content":"trying"}`,
		`{"type":"error","message":"provider auth failed"}`,
	}, "\n")

	result := b.processEvents(strings.NewReader(lines), ch)

	if result.status != "failed" {
		t.Errorf("status: got %q, want %q", result.status, "failed")
	}
	if result.errMsg != "provider auth failed" {
		t.Errorf("errMsg: got %q", result.errMsg)
	}

	close(ch)
	var errs int
	for m := range ch {
		if m.Type == MessageError {
			errs++
		}
	}
	if errs != 1 {
		t.Errorf("expected 1 error message, got %d", errs)
	}
}

func TestEvosciProcessEventsSkipsNonJSONNoise(t *testing.T) {
	t.Parallel()

	b := &evosciBackend{cfg: Config{Logger: slog.Default()}}
	ch := make(chan Message, 256)

	// A stray non-JSON line (e.g. an MCP server banner that leaked to stdout)
	// must be skipped, not abort the stream.
	lines := strings.Join([]string{
		`Context7 MCP Server running on stdio`,
		`{"type":"text","content":"valid"}`,
		`{"type":"done","content":"valid","response":"valid"}`,
	}, "\n")

	result := b.processEvents(strings.NewReader(lines), ch)

	if result.status != "completed" {
		t.Errorf("status: got %q, want %q", result.status, "completed")
	}
	if result.output != "valid" {
		t.Errorf("output: got %q, want %q", result.output, "valid")
	}
}

func TestEvosciProcessEventsSubagentMappedToToolMessages(t *testing.T) {
	t.Parallel()

	b := &evosciBackend{cfg: Config{Logger: slog.Default()}}
	ch := make(chan Message, 256)

	lines := strings.Join([]string{
		`{"type":"subagent_start","name":"researcher","description":"dig into X"}`,
		`{"type":"subagent_tool_call","subagent":"researcher","name":"search","args":{"q":"x"},"id":"sc_1"}`,
		`{"type":"subagent_tool_result","subagent":"researcher","name":"search","content":"found","success":true,"id":"sc_1"}`,
		`{"type":"subagent_text","subagent":"researcher","content":"summary","instance_id":"i1"}`,
		`{"type":"done","content":"ok","response":"ok"}`,
	}, "\n")

	result := b.processEvents(strings.NewReader(lines), ch)
	if result.status != "completed" {
		t.Errorf("status: got %q, want completed", result.status)
	}

	close(ch)
	var toolUse, toolResult, text int
	for m := range ch {
		switch m.Type {
		case MessageToolUse:
			toolUse++
		case MessageToolResult:
			toolResult++
		case MessageText:
			text++
		}
	}
	if toolUse != 1 {
		t.Errorf("tool-use messages: got %d, want 1", toolUse)
	}
	if toolResult != 1 {
		t.Errorf("tool-result messages: got %d, want 1", toolResult)
	}
	if text != 1 {
		t.Errorf("text messages (subagent_text): got %d, want 1", text)
	}
}
