package bootstrap

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

// UploadKibanaDashboards imports a saved-objects bundle (ndjson) into Kibana.
// Waits for Kibana's /api/status to report "available" before uploading.
//
// kibanaURL is the base URL (e.g. "http://kibana:5601"). exportPath is a local
// filesystem path to a .ndjson file produced by Kibana's saved-objects export.
func UploadKibanaDashboards(kibanaURL, exportPath string) error {
	statusURL := kibanaURL + "/api/status"
	if err := WaitForReady("Kibana", statusURL, 5*time.Minute, kibanaReady); err != nil {
		return err
	}

	importURL := kibanaURL + "/api/saved_objects/_import?createNewCopies=false"
	return uploadMultipart(importURL, map[string]string{"kbn-xsrf": "true"}, "file", exportPath)
}

func kibanaReady(r *http.Response) bool {
	if r.StatusCode != http.StatusOK {
		return false
	}
	var body struct {
		Status struct {
			Overall struct {
				Level string `json:"level"`
			} `json:"overall"`
		} `json:"status"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		return false
	}
	return body.Status.Overall.Level == "available"
}

func uploadMultipart(url string, headers map[string]string, paramName, path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	var b bytes.Buffer
	writer := multipart.NewWriter(&b)
	part, err := writer.CreateFormFile(paramName, filepath.Base(path))
	if err != nil {
		return err
	}
	if _, err := io.Copy(part, file); err != nil {
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}

	req, err := http.NewRequest("POST", url, &b)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode >= 400 {
		return fmt.Errorf("kibana import %s: %s", resp.Status, string(body))
	}
	return nil
}
