package dataplane

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"

	lumv1 "github.com/alDuncanson/lum/control-plane/internal/gen/lum/v1"
)

type mismatchedDataPlane struct {
	lumv1.UnimplementedDataPlaneServer
}

func (mismatchedDataPlane) Health(context.Context, *lumv1.HealthRequest) (*lumv1.HealthResponse, error) {
	return &lumv1.HealthResponse{Ready: true, ContractVersion: "old"}, nil
}

func TestSupervisorReapsUnexpectedExit(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("test helper is a shell script")
	}

	bin := filepath.Join(t.TempDir(), "lumen")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\nexit 7\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	sup, err := Spawn(bin, t.TempDir(), "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-sup.done:
	case <-time.After(5 * time.Second):
		t.Fatal("child exit was not reaped")
	}
	if sup.cmd.ProcessState == nil || !sup.cmd.ProcessState.Exited() {
		t.Fatal("child has no exited process state after supervisor completed")
	}

	// Stop remains safe after an unexpected exit and must not call Wait twice.
	sup.Stop()
}

func TestWaitReadyRejectsContractMismatch(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	lumv1.RegisterDataPlaneServer(server, mismatchedDataPlane{})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})

	client, err := Dial(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })

	err = client.WaitReady(context.Background(), time.Second)
	if err == nil || !strings.Contains(err.Error(), "contract version mismatch") {
		t.Fatalf("WaitReady error = %v, want contract version mismatch", err)
	}
}
