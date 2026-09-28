package quality

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestWithoutPollDeadlineIgnoresCancel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	cancel()
	got := withoutPollDeadline(ctx)
	if err := got.Err(); err != nil {
		t.Fatalf("poll cancel must not follow the session: %v", err)
	}
}

func TestIsMissingObject(t *testing.T) {
	if !isMissingObject(fmt.Errorf("002003 (02000): SQL compilation error: Schema 'ALPHA_DB.QUALITY' does not exist or not authorized.")) {
		t.Fatal("expected missing validation schema")
	}
	if isMissingObject(fmt.Errorf("timeout waiting for warehouse")) {
		t.Fatal("timeout is not a missing object")
	}
	if isMissingObject(nil) {
		t.Fatal("nil")
	}
}

func TestMissingRelationCache(t *testing.T) {
	s := &Snowflake{}
	rel := "ALPHA_DB.QUALITY.VALIDATION_RESULT"
	if s.knownMissing(rel) {
		t.Fatal("empty cache")
	}
	s.markMissing(rel)
	if !s.knownMissing(rel) {
		t.Fatal("expected cached miss")
	}
}

func TestMissingObjectLogWriterDrops002003(t *testing.T) {
	var buf strings.Builder
	w := missingObjectLogWriter{w: &buf}
	n, err := w.Write([]byte("ERRO[1] 002003 (02000): Schema 'ALPHA_DB.QUALITY' does not exist or not authorized.\n"))
	if err != nil || n == 0 {
		t.Fatalf("write: n=%d err=%v", n, err)
	}
	if buf.Len() != 0 {
		t.Fatalf("expected drop, got %q", buf.String())
	}
	if _, err := w.Write([]byte("ERRO[1] authentication timed out\n")); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "authentication timed out") {
		t.Fatalf("kept log %q", buf.String())
	}
}
