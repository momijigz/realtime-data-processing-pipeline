// Package bootstrap handles one-shot pipeline setup: uploading Kibana
// dashboards, creating the Elasticsearch sink connector, and waiting for
// dependent services to be HTTP-ready. These operations are idempotent in
// spirit — running bootstrap twice should not corrupt state.
package bootstrap

import (
	"fmt"
	"net/http"
	"time"
)

// WaitForReady polls `url` every 2s until `ready(resp)` returns true or
// `timeout` elapses. Returns nil on first ready response; non-nil error on
// deadline.
func WaitForReady(name, url string, timeout time.Duration, ready func(*http.Response) bool) error {
	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: 5 * time.Second}
	attempt := 0
	for {
		attempt++
		resp, err := client.Get(url)
		if err == nil {
			ok := ready(resp)
			resp.Body.Close()
			if ok {
				fmt.Printf("[ %s ] ready after %d attempt(s)\n", name, attempt)
				return nil
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("[ %s ] not ready after %s (last err: %v)", name, timeout, err)
		}
		time.Sleep(2 * time.Second)
	}
}
