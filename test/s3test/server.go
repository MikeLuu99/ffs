// Package s3test provides an in-process S3 HTTP fixture. Requests still pass
// through the real AWS SDK, signing, range parsing, and XML pagination logic.
package s3test

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"

	"example.com/dfs/internal/storage/s3"
)

type Server struct {
	mu         sync.Mutex
	objects    map[string][]byte
	failWrites bool
}

func New() *Server { return &Server{objects: make(map[string][]byte)} }

func (s *Server) Config(prefix string) s3.Config {
	return s3.Config{Endpoint: "https://s3.test", Bucket: "ffs", Region: "auto", Prefix: prefix,
		AccessKeyID: "test", SecretAccessKey: "test-secret", HTTPClient: &http.Client{Transport: s}}
}

func (s *Server) FailWrites(fail bool) { s.mu.Lock(); defer s.mu.Unlock(); s.failWrites = fail }

func (s *Server) RoundTrip(request *http.Request) (*http.Response, error) {
	if err := request.Context().Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	w := httptest.NewRecorder()
	s.serve(w, request)
	return w.Result(), nil
}

func failure(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = fmt.Fprintf(w, "<Error><Code>%s</Code><Message>fixture error</Message></Error>", code)
}

func (s *Server) serve(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256 Credential=test/") || !strings.Contains(r.Header.Get("Authorization"), "/auto/s3/aws4_request") {
		failure(w, http.StatusForbidden, "SignatureDoesNotMatch")
		return
	}
	if r.URL.Path == "/ffs" || r.URL.Path == "/ffs/" {
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2" {
			s.list(w, r)
			return
		}
	}
	if !strings.HasPrefix(r.URL.Path, "/ffs/") {
		failure(w, http.StatusNotFound, "NoSuchBucket")
		return
	}
	key := strings.TrimPrefix(r.URL.Path, "/ffs/")
	data, exists := s.objects[key]
	switch r.Method {
	case http.MethodPut:
		if s.failWrites {
			failure(w, http.StatusForbidden, "AccessDenied")
			return
		}
		if r.Header.Get("If-None-Match") != "*" {
			failure(w, http.StatusBadRequest, "InvalidRequest")
			return
		}
		if exists {
			failure(w, http.StatusPreconditionFailed, "PreconditionFailed")
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			failure(w, http.StatusBadRequest, "InvalidRequest")
			return
		}
		s.objects[key] = bytes.Clone(body)
		w.Header().Set("ETag", `"fixture"`)
	case http.MethodGet, http.MethodHead:
		if !exists {
			failure(w, http.StatusNotFound, "NoSuchKey")
			return
		}
		if r.Method == http.MethodHead {
			w.Header().Set("Content-Length", fmt.Sprint(len(data)))
			return
		}
		var start, end int
		if _, err := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &start, &end); err != nil || start < 0 || end < start || end >= len(data) {
			failure(w, http.StatusRequestedRangeNotSatisfiable, "InvalidRange")
			return
		}
		w.Header().Set("Content-Length", fmt.Sprint(end-start+1))
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(data[start : end+1])
	case http.MethodDelete:
		delete(s.objects, key)
		w.WriteHeader(http.StatusNoContent)
	default:
		failure(w, http.StatusMethodNotAllowed, "MethodNotAllowed")
	}
}

func (s *Server) list(w http.ResponseWriter, r *http.Request) {
	var keys []string
	prefix, token := r.URL.Query().Get("prefix"), r.URL.Query().Get("continuation-token")
	for key := range s.objects {
		if strings.HasPrefix(key, prefix) && key > token {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	result := struct {
		XMLName   xml.Name `xml:"ListBucketResult"`
		Truncated bool     `xml:"IsTruncated"`
		Token     string   `xml:"NextContinuationToken,omitempty"`
		Contents  []struct {
			Key string `xml:"Key"`
		} `xml:"Contents"`
	}{}
	if len(keys) > 2 {
		keys = keys[:2]
		result.Truncated = true
		result.Token = keys[1]
	}
	for _, key := range keys {
		result.Contents = append(result.Contents, struct {
			Key string `xml:"Key"`
		}{key})
	}
	w.Header().Set("Content-Type", "application/xml")
	_ = xml.NewEncoder(w).Encode(result)
}
