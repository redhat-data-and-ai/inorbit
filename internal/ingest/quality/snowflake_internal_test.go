package quality

import (
	"context"
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
