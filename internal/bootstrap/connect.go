package bootstrap

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// ConnectorConfig matches the Kafka Connect REST API request body.
type ConnectorConfig struct {
	Name   string            `json:"name"`
	Config map[string]string `json:"config"`
}

// CreateESConnector waits for Kafka Connect's REST API to be ready, then
// POSTs an Elasticsearch sink connector configuration. Returns nil on success
// (HTTP 201) or if the connector already exists (HTTP 409). Other failures
// return a non-nil error.
//
// connectURL is the base URL (e.g. "http://kafka-connect:8083").
// topic is the Kafka source topic; esURL is the destination Elasticsearch URL.
func CreateESConnector(connectURL, topic, esURL string) error {
	connectorsURL := connectURL + "/connectors"
	if err := WaitForReady("Kafka Connect", connectorsURL, 5*time.Minute, func(r *http.Response) bool {
		return r.StatusCode == http.StatusOK
	}); err != nil {
		return err
	}

	cfg := ConnectorConfig{
		Name: "elasticsearch-sink",
		Config: map[string]string{
			"connector.class":                "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
			"tasks.max":                      "1",
			"topics":                         topic,
			"key.ignore":                     "true",
			"schema.ignore":                  "true",
			"connection.url":                 esURL,
			"type.name":                      "_doc",
			"name":                           "elasticsearch-sink",
			"value.converter":                "org.apache.kafka.connect.json.JsonConverter",
			"value.converter.schemas.enable": "false",
		},
	}

	payload, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshal connector cfg: %w", err)
	}
	req, err := http.NewRequest("POST", connectorsURL, bytes.NewBuffer(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusCreated, http.StatusConflict:
		// 201 = created, 409 = already exists. Both are fine for an idempotent bootstrap.
		return nil
	default:
		var body map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&body)
		return fmt.Errorf("create connector: %s: %v", resp.Status, body)
	}
}
