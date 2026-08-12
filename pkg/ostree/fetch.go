package ostree

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
)

// RemoteConfig describes where and how to fetch objects from. BaseURL may be an
// http(s):// or file:// URL.
//
// fiopull is intentionally Device-Gateway-agnostic: it fetches from a plain
// HTTP(s) server (e.g. a signed object-store / GCS URL) and does not speak the
// gateway's download-urls protocol or use device mTLS credentials. Any auth is
// carried by the caller through the URL itself (a signed URL) or via Headers
// (e.g. "Authorization: Bearer ..."). Headers are sent on every request.
type RemoteConfig struct {
	BaseURL string
	Headers map[string]string
}

// transport abstracts fetching a repo-relative path, so http:// and file://
// share the pull logic.
type transport interface {
	// open returns a reader for relPath starting at byte offset (0 = whole
	// object). resumed reports whether the server honored the offset (so the
	// caller should append rather than truncate). A nil error with resumed=false
	// and offset>0 means the server ignored Range and is sending from the start.
	open(ctx context.Context, relPath string, offset int64) (rc io.ReadCloser, resumed bool, err error)
}

// newTransport builds a transport for the remote's BaseURL.
func newTransport(rc RemoteConfig) (transport, error) {
	u, err := url.Parse(rc.BaseURL)
	if err != nil {
		return nil, fmt.Errorf("bad remote URL %q: %w", rc.BaseURL, err)
	}
	switch u.Scheme {
	case "file", "":
		return &fileTransport{root: u.Path}, nil
	case "http", "https":
		base := strings.TrimRight(rc.BaseURL, "/")
		return &httpTransport{client: &http.Client{}, base: base, headers: rc.Headers}, nil
	default:
		return nil, fmt.Errorf("unsupported remote scheme %q", u.Scheme)
	}
}

// --- HTTP transport ---

type httpTransport struct {
	client  *http.Client
	base    string
	headers map[string]string
}

func (t *httpTransport) open(ctx context.Context, relPath string, offset int64) (io.ReadCloser, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, t.base+"/"+relPath, nil)
	if err != nil {
		return nil, false, err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	for k, v := range t.headers {
		req.Header.Set(k, v)
	}
	if offset > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", offset))
	}
	resp, err := t.client.Do(req)
	if err != nil {
		return nil, false, err
	}
	switch resp.StatusCode {
	case http.StatusOK:
		// Whole object (server ignored Range if we sent one).
		return resp.Body, false, nil
	case http.StatusPartialContent:
		return resp.Body, true, nil
	default:
		resp.Body.Close()
		return nil, false, &httpError{path: relPath, status: resp.StatusCode}
	}
}

// httpError reports a non-success HTTP status for a fetched path.
type httpError struct {
	path   string
	status int
}

func (e *httpError) Error() string {
	return fmt.Sprintf("fetch %s: HTTP %d", e.path, e.status)
}

// isNotFound reports whether err indicates a missing object (HTTP 404 or a
// missing file://), so callers can tolerate optional objects.
func isNotFound(err error) bool {
	if he, ok := err.(*httpError); ok {
		return he.status == http.StatusNotFound
	}
	if _, ok := err.(*fileNotFound); ok {
		return true
	}
	return false
}

// isRangeNotSatisfiable reports whether err is an HTTP 416 response. This
// happens when we resume a .part sidecar whose size is at/beyond the object's
// length (e.g. a previous run downloaded the object in full but was killed
// before the part was consumed and removed): the Range: bytes=<size>- request
// starts past the end. The caller recovers by refetching from offset 0.
func isRangeNotSatisfiable(err error) bool {
	if he, ok := err.(*httpError); ok {
		return he.status == http.StatusRequestedRangeNotSatisfiable
	}
	return false
}

// --- file:// transport ---

type fileTransport struct{ root string }

func (t *fileTransport) open(_ context.Context, relPath string, offset int64) (io.ReadCloser, bool, error) {
	f, err := os.Open(filepath.Join(t.root, filepath.FromSlash(relPath)))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, false, &fileNotFound{relPath}
		}
		return nil, false, err
	}
	if offset > 0 {
		if _, err := f.Seek(offset, io.SeekStart); err != nil {
			f.Close()
			return nil, false, err
		}
		return f, true, nil
	}
	return f, false, nil
}

type fileNotFound struct{ path string }

func (e *fileNotFound) Error() string { return "not found: " + e.path }

// objectRelPath returns the repo-relative URL path of a loose object.
func objectRelPath(csum, ext string) string {
	return path.Join("objects", csum[:2], csum[2:]+"."+ext)
}
