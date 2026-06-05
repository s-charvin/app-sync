package smoke

import (
	"encoding/json"
	"net/http"
	"os"
	"testing"
)

const defaultBaseURL = "https://sync.thingcue.top"

func getBaseURL() string {
	if u := os.Getenv("APP_SYNC_URL"); u != "" {
		return u
	}
	return defaultBaseURL
}

type healthResponse struct {
	Status  string `json:"status"`
	Version string `json:"version"`
	AppName string `json:"app_name"`
}

func TestSmoke_HealthEndpoint(t *testing.T) {
	url := getBaseURL() + "/syncx/health"
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s failed: %v", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}

	var body healthResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if body.Status != "healthy" {
		t.Errorf("expected status healthy, got %s", body.Status)
	}
	if body.AppName == "" {
		t.Error("app_name should not be empty")
	}
}

func TestSmoke_CapabilitiesEndpoint(t *testing.T) {
	url := getBaseURL() + "/sync/capabilities"
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s failed: %v", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 401 && resp.StatusCode != 200 {
		t.Errorf("expected 401 or 200, got %d", resp.StatusCode)
	}
}

func TestSmoke_HealthEndpointReturnsJSON(t *testing.T) {
	url := getBaseURL() + "/syncx/health"
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s failed: %v", url, err)
	}
	defer resp.Body.Close()

	ct := resp.Header.Get("Content-Type")
	if ct != "application/json" {
		t.Errorf("expected Content-Type application/json, got %s", ct)
	}
}

func TestSmoke_StatusEndpoint(t *testing.T) {
	url := getBaseURL() + "/syncx/status"
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s failed: %v", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
}

func TestSmoke_HealthEndpointFromServer(t *testing.T) {
	url := getBaseURL() + "/health"
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s failed: %v", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}

	var body map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if body["status"] != "ok" {
		t.Errorf("expected status ok, got %v", body["status"])
	}
	if body["app"] != "app-sync" {
		t.Errorf("expected app app-sync, got %v", body["app"])
	}
}
