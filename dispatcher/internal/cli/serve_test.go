package cli

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestResetTimerExtendsIdleDeadline(t *testing.T) {
	timer := time.NewTimer(100 * time.Millisecond)
	defer timer.Stop()
	time.Sleep(20 * time.Millisecond)
	resetTimer(timer, 200*time.Millisecond)

	select {
	case <-timer.C:
		t.Fatal("timer fired at its original deadline")
	case <-time.After(120 * time.Millisecond):
	}
	select {
	case <-timer.C:
	case <-time.After(150 * time.Millisecond):
		t.Fatal("reset timer did not fire")
	}
}

func TestDaemonLockCoversCompleteLifetime(t *testing.T) {
	path := filepath.Join(t.TempDir(), "daemon.lock")
	first, err := acquireDaemonLock(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := acquireDaemonLock(ctx, path); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second lock error = %v, want deadline exceeded", err)
	}
	_ = first.Close()

	third, err := acquireDaemonLock(context.Background(), path)
	if err != nil {
		t.Fatalf("acquiring lock after daemon cleanup: %v", err)
	}
	_ = third.Close()
}
