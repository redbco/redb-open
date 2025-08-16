package databases

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"
)

func TestParseArg_LastWins(t *testing.T) {
	args := []string{"--username=old", "--username=new"}
	val, ok := parseArg(args, usernameKey)
	if !ok || val != "new" {
		t.Fatalf("want ok,new; got ok=%v val=%q", ok, val)
	}
}

func TestParseArg_NotFound(t *testing.T) {
	args := []string{"--other=42"}
	if _, ok := parseArg(args, usernameKey); ok {
		t.Fatal("expected ok=false for missing key")
	}
}

func TestParseArg_TrimsSpaces(t *testing.T) {
	args := []string{"--username=  alice  "}
	val, ok := parseArg(args, usernameKey)
	if !ok || val != "alice" {
		t.Fatalf("want ok,alice; got ok=%v val=%q", ok, val)
	}
}

func TestParseArg_RequiresEqualsForm(t *testing.T) {
	args := []string{"--username", "bob"}
	if _, ok := parseArg(args, usernameKey); ok {
		t.Fatal("expected ok=false for '--key value' form")
	}
}

func TestScanArgs_BasicAndUnknownIgnored(t *testing.T) {
	args := []string{
		"--username=u1",
		"--port=5432",
		"--unknown=zzz",
		"--db-name=main",
	}
	m := scanArgs(args)

	if got := m[usernameKey]; got != "u1" {
		t.Fatalf("username want u1 got %q", got)
	}
	if got := m[portKey]; got != "5432" {
		t.Fatalf("port want 5432 got %q", got)
	}
	if _, ok := m[argKey("unknown")]; ok {
		t.Fatal("unknown key must be ignored")
	}
	if got := m[dbNameKey]; got != "main" {
		t.Fatalf("db-name want main got %q", got)
	}
}

func TestScanArgs_LastWinsPerKey(t *testing.T) {
	args := []string{
		"--vendor=aws",
		"--vendor=gcp",
		"--vendor=azure",
	}
	m := scanArgs(args)
	if got := m[dbVendorKey]; got != "azure" {
		t.Fatalf("vendor want azure got %q", got)
	}
}

func captureStdout(fn func()) string {
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	fn()
	_ = w.Close()
	os.Stdout = old
	var buf bytes.Buffer
	_, _ = io.Copy(&buf, r)
	return buf.String()
}

func TestGetArgOrPrompt_FromMapNonEmpty(t *testing.T) {
	m := map[argKey]string{instanceKey: "inst-1"}
	got := getArgOrPrompt(rdr("ignored\n"), m, instanceKey, "Q?", true)
	if got != "inst-1" {
		t.Fatalf("want inst-1 got %q", got)
	}
}

func TestGetArgOrPrompt_FromMapEmptyThenPrompt(t *testing.T) {
	m := map[argKey]string{instanceKey: "   "}
	out := captureStdout(func() {
		got := getArgOrPrompt(rdr("inst-from-prompt\n"), m, instanceKey, "Enter instance: ", true)
		if got != "inst-from-prompt" {
			t.Fatalf("want inst-from-prompt got %q", got)
		}
	})
	if !strings.Contains(out, "Enter instance: ") {
		t.Fatalf("expected question to be printed, got %q", out)
	}
}

func TestGetArgOrPrompt_MissingNoPrompt(t *testing.T) {
	m := map[argKey]string{}
	out := captureStdout(func() {
		got := getArgOrPrompt(rdr("will-not-be-read\n"), m, descriptionKey, "Ask: ", false)
		if got != "" {
			t.Fatalf("want empty got %q", got)
		}
	})
	if out != "" {
		t.Fatalf("stdout should be empty when promptIfMissing=false, got %q", out)
	}
}

func TestPrompt_ReadsAndTrims_WithNewline(t *testing.T) {
	out := captureStdout(func() {
		got := prompt(rdr("  value \n"), "Test Q: ")
		if got != "value" {
			t.Fatalf("want 'value' got %q", got)
		}
	})
	if !strings.Contains(out, "Test Q: ") {
		t.Fatalf("expected question printed, got %q", out)
	}
}

func TestPrompt_ReadsWithoutNewline(t *testing.T) {
	got := prompt(rdr("no-newline"), "Q: ")
	if got != "no-newline" {
		t.Fatalf("want 'no-newline' got %q", got)
	}
}
