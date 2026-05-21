package main

import (
	"os"
	"testing"
	"time"
)

func TestResolveProjectIDUsesFlagValue(t *testing.T) {
	t.Setenv("GCP_PROJECT_ID", "env-project")

	got, err := resolveProjectID("flag-project")
	if err != nil {
		t.Fatalf("resolveProjectID returned error: %v", err)
	}
	if got != "flag-project" {
		t.Fatalf("resolveProjectID = %q, want %q", got, "flag-project")
	}
}

func TestResolveProjectIDFallsBackToEnvironment(t *testing.T) {
	t.Setenv("GCP_PROJECT_ID", "env-project")

	got, err := resolveProjectID("")
	if err != nil {
		t.Fatalf("resolveProjectID returned error: %v", err)
	}
	if got != "env-project" {
		t.Fatalf("resolveProjectID = %q, want %q", got, "env-project")
	}
}

func TestResolveProjectIDErrorsWhenUnset(t *testing.T) {
	old := os.Getenv("GCP_PROJECT_ID")
	_ = os.Unsetenv("GCP_PROJECT_ID")
	defer func() {
		if old != "" {
			_ = os.Setenv("GCP_PROJECT_ID", old)
		}
	}()

	_, err := resolveProjectID("")
	if err == nil {
		t.Fatal("resolveProjectID returned nil error, want error")
	}
}

func TestParseDurationFlagParsesValue(t *testing.T) {
	got, err := parseDurationFlag("15s")
	if err != nil {
		t.Fatalf("parseDurationFlag returned error: %v", err)
	}
	if got != 15*time.Second {
		t.Fatalf("parseDurationFlag = %v, want %v", got, 15*time.Second)
	}
}
