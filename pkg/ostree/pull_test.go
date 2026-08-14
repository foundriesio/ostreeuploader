//go:build linux

package ostree

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// zeroTime disables Last-Modified handling in http.ServeContent.
var zeroTime = time.Time{}

// TestPullEndToEnd pulls a commit from an archive repo served over HTTP into a
// fresh bare-user repo, then validates it with `ostree fsck` and compares the
// file listing against the source.
func TestPullEndToEnd(t *testing.T) {
	requireOstree(t)
	srcRepo, ref, commit := makeContentRepo(t)
	srv := httptest.NewServer(http.FileServer(http.Dir(srcRepo)))
	defer srv.Close()

	dest := filepath.Join(t.TempDir(), "dest")
	r := OpenRepo(dest)
	res, err := r.Pull(context.Background(), PullOptions{
		Remote: RemoteConfig{BaseURL: srv.URL},
		Ref:    ref,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Commit != commit {
		t.Fatalf("pulled commit %s want %s", res.Commit, commit)
	}
	if res.ContentFetched == 0 || res.MetaFetched == 0 {
		t.Fatalf("expected objects fetched, got %+v", res)
	}

	// fsck must pass on the destination bare-user repo.
	out := run(t, "ostree", "--repo="+dest, "fsck")
	if !strings.Contains(out, "no errors found") {
		t.Errorf("fsck output unexpected:\n%s", out)
	}
	// The pulled ref resolves to the same commit.
	got, err := r.ResolveRef(ref)
	if err != nil || got != commit {
		t.Fatalf("ResolveRef=%q,%v want %s", got, err, commit)
	}
	// Listings match.
	srcLs := run(t, "ostree", "--repo="+srcRepo, "ls", "-R", commit)
	dstLs := run(t, "ostree", "--repo="+dest, "ls", "-R", commit)
	if srcLs != dstLs {
		t.Errorf("listings differ:\n--- src ---\n%s\n--- dst ---\n%s", srcLs, dstLs)
	}
}

// TestPullObjectLevelResume verifies a re-run after losing the ref + partial
// marker (but keeping objects) re-fetches nothing.
func TestPullObjectLevelResume(t *testing.T) {
	requireOstree(t)
	srcRepo, ref, _ := makeContentRepo(t)
	srv := httptest.NewServer(http.FileServer(http.Dir(srcRepo)))
	defer srv.Close()

	dest := filepath.Join(t.TempDir(), "dest")
	r := OpenRepo(dest)
	opts := PullOptions{Remote: RemoteConfig{BaseURL: srv.URL}, Ref: ref}
	if _, err := r.Pull(context.Background(), opts); err != nil {
		t.Fatal(err)
	}
	// Second run: everything present -> all skipped, nothing fetched.
	res, err := r.Pull(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	if res.ContentFetched != 0 || res.MetaFetched != 0 {
		t.Errorf("re-run fetched objects, expected all skipped: %+v", res)
	}
	if res.ObjectsSkipped == 0 {
		t.Errorf("expected skips on re-run, got %+v", res)
	}
}

// flakyServer serves files but, for the first request whose path contains
// trigger, cuts the response off after maxBytes bytes (and does NOT honor Range
// on that first hit) to simulate a dropped connection mid-object.
type flakyServer struct {
	root     string
	trigger  string
	maxBytes int

	mu      sync.Mutex
	tripped bool
	ranges  []string // recorded Range headers for the trigger path
}

func (s *flakyServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rel := strings.TrimPrefix(r.URL.Path, "/")
	full := filepath.Join(s.root, filepath.FromSlash(rel))
	data, err := os.ReadFile(full)
	if err != nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	isTrigger := strings.Contains(rel, s.trigger)
	if isTrigger {
		s.mu.Lock()
		s.ranges = append(s.ranges, r.Header.Get("Range"))
		first := !s.tripped
		s.tripped = true
		s.mu.Unlock()
		if first {
			// Truncate: send a short prefix then stop (connection closes when
			// the handler returns without writing the rest).
			w.Header().Set("Content-Length", itoa(len(data)))
			w.WriteHeader(http.StatusOK)
			n := s.maxBytes
			if n > len(data) {
				n = len(data)
			}
			w.Write(data[:n])
			if fl, ok := w.(http.Flusher); ok {
				fl.Flush()
			}
			// Hijack and close to force the client to see a short read.
			if hj, ok := w.(http.Hijacker); ok {
				if conn, _, err := hj.Hijack(); err == nil {
					conn.Close()
				}
			}
			return
		}
	}
	// Normal path (and all retries): honor Range via ServeContent.
	http.ServeContent(w, r, filepath.Base(full), zeroTime, strings.NewReader(string(data)))
}

// TestPullByteLevelResume is the headline test: an object download is cut off
// mid-stream; the retry must resume via a Range request rather than restart.
func TestPullByteLevelResume(t *testing.T) {
	requireOstree(t)
	srcRepo, ref, commit := makeContentRepo(t)

	// The large object is the random 1.5MB "prog" file; target its .filez.
	r0 := OpenRepo(srcRepo)
	bigCsum := largestFileObject(t, r0, commit)
	fs := &flakyServer{root: srcRepo, trigger: bigCsum[2:] + ".filez", maxBytes: 4096}
	srv := httptest.NewServer(fs)
	defer srv.Close()

	dest := filepath.Join(t.TempDir(), "dest")
	r := OpenRepo(dest)
	opts := PullOptions{Remote: RemoteConfig{BaseURL: srv.URL}, Ref: ref, Concurrency: 1}

	// First attempt: should fail on the truncated object.
	_, err := r.Pull(context.Background(), opts)
	if err == nil {
		t.Fatal("expected first pull to fail on truncated object")
	}
	// A .part sidecar should remain with the partial bytes.
	part := filepath.Join(dest, "tmp", bigCsum+partFileSuffix)
	fi, perr := os.Stat(part)
	if perr != nil {
		t.Fatalf("expected partial sidecar %s: %v", part, perr)
	}
	if fi.Size() == 0 {
		t.Fatal("partial sidecar is empty")
	}

	// Second attempt against the now-healthy server: must succeed and must have
	// issued a Range request for the resumed object.
	res, err := r.Pull(context.Background(), opts)
	if err != nil {
		t.Fatalf("resume pull failed: %v", err)
	}
	fs.mu.Lock()
	ranges := append([]string(nil), fs.ranges...)
	fs.mu.Unlock()
	sawRange := false
	for _, rg := range ranges {
		if strings.HasPrefix(rg, "bytes=") && rg != "bytes=0-" {
			sawRange = true
		}
	}
	if !sawRange {
		t.Errorf("expected a resume Range request, recorded ranges: %v", ranges)
	}
	if res.Commit != commit {
		t.Fatalf("commit %s want %s", res.Commit, commit)
	}
	out := run(t, "ostree", "--repo="+dest, "fsck")
	if !strings.Contains(out, "no errors found") {
		t.Errorf("fsck after resume failed:\n%s", out)
	}
}

// TestPullRangeIgnoredFallback ensures correctness when the server ignores Range
// and always returns 200 (the stdlib FileServer with a pre-seeded .part).
func TestPullRangeIgnoredFallback(t *testing.T) {
	requireOstree(t)
	srcRepo, ref, commit := makeContentRepo(t)
	// A plain handler that always returns the whole file with 200, never 206.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rel := strings.TrimPrefix(r.URL.Path, "/")
		data, err := os.ReadFile(filepath.Join(srcRepo, filepath.FromSlash(rel)))
		if err != nil {
			http.Error(w, "nf", http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK) // ignore any Range
		w.Write(data)
	}))
	defer srv.Close()

	dest := filepath.Join(t.TempDir(), "dest")
	r := OpenRepo(dest)
	// Pre-seed a bogus partial for the big object to exercise the truncate path.
	bigCsum := largestFileObject(t, OpenRepo(srcRepo), commit)
	os.MkdirAll(filepath.Join(dest, "tmp"), 0o755)
	os.WriteFile(filepath.Join(dest, "tmp", bigCsum+partFileSuffix), []byte("garbage"), 0o644)

	res, err := r.Pull(context.Background(), PullOptions{Remote: RemoteConfig{BaseURL: srv.URL}, Ref: ref})
	if err != nil {
		t.Fatalf("pull with Range-ignoring server failed: %v", err)
	}
	if res.Commit != commit {
		t.Fatalf("commit mismatch")
	}
	out := run(t, "ostree", "--repo="+dest, "fsck")
	if !strings.Contains(out, "no errors found") {
		t.Errorf("fsck failed:\n%s", out)
	}
}

// TestPullResumePartAtFullSize covers the case where a prior run left a .part
// sidecar whose size equals (or exceeds) the object's full length — e.g. the
// object finished downloading but the process was killed before the part was
// consumed and removed. Resuming with Range: bytes=<size>- makes a Range-honoring
// server (like GCS) reply 416; the pull must recover by refetching from 0.
func TestPullResumePartAtFullSize(t *testing.T) {
	requireOstree(t)
	srcRepo, ref, commit := makeContentRepo(t)
	// A server that honors Range via ServeContent, so an out-of-range resume
	// start yields a real 416 (the stdlib FileServer behaves the same way).
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rel := strings.TrimPrefix(r.URL.Path, "/")
		data, err := os.ReadFile(filepath.Join(srcRepo, filepath.FromSlash(rel)))
		if err != nil {
			http.Error(w, "nf", http.StatusNotFound)
			return
		}
		http.ServeContent(w, r, filepath.Base(rel), zeroTime, strings.NewReader(string(data)))
	}))
	defer srv.Close()

	dest := filepath.Join(t.TempDir(), "dest")
	r := OpenRepo(dest)

	// Pre-seed a .part for the big object at its exact full size, so the resume
	// Range starts at the end of the object and the server returns 416.
	bigCsum := largestFileObject(t, OpenRepo(srcRepo), commit)
	srcPart, err := os.ReadFile(OpenRepo(srcRepo).objectPath(bigCsum, "filez"))
	if err != nil {
		t.Fatal(err)
	}
	os.MkdirAll(filepath.Join(dest, "tmp"), 0o755)
	if err := os.WriteFile(filepath.Join(dest, "tmp", bigCsum+partFileSuffix), srcPart, 0o644); err != nil {
		t.Fatal(err)
	}

	res, err := r.Pull(context.Background(), PullOptions{Remote: RemoteConfig{BaseURL: srv.URL}, Ref: ref, Concurrency: 1})
	if err != nil {
		t.Fatalf("pull with full-size .part (416 resume) failed: %v", err)
	}
	if res.Commit != commit {
		t.Fatalf("commit %s want %s", res.Commit, commit)
	}
	out := run(t, "ostree", "--repo="+dest, "fsck")
	if !strings.Contains(out, "no errors found") {
		t.Errorf("fsck after 416 recovery failed:\n%s", out)
	}
}

// TestPullRemovesStalePartsFromDifferentPull verifies that a leftover .part
// sidecar from an earlier, abandoned pull (whose checksum this commit does not
// reference) is removed, while a .part that IS referenced by this commit is
// kept so its bytes resume.
func TestPullRemovesStalePartsFromDifferentPull(t *testing.T) {
	requireOstree(t)
	srcRepo, ref, commit := makeContentRepo(t)
	srv := httptest.NewServer(http.FileServer(http.Dir(srcRepo)))
	defer srv.Close()

	dest := filepath.Join(t.TempDir(), "dest")
	r := OpenRepo(dest)

	// Seed tmp/ with two sidecars before the pull:
	//  - a "stale" one with a checksum this commit does not reference (leftover
	//    from a different, interrupted pull) -> must be removed.
	//  - a "relevant" one whose checksum IS a content object of this commit,
	//    pre-filled with a partial prefix -> must be kept (byte-level resume).
	tmpDir := filepath.Join(dest, "tmp")
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		t.Fatal(err)
	}
	staleCsum := "0000000000000000000000000000000000000000000000000000000000000001"
	stalePart := filepath.Join(tmpDir, staleCsum+partFileSuffix)
	if err := os.WriteFile(stalePart, []byte("leftover"), 0o644); err != nil {
		t.Fatal(err)
	}
	relevantCsum := largestFileObject(t, OpenRepo(srcRepo), commit)
	relevantPart := filepath.Join(tmpDir, relevantCsum+partFileSuffix)
	// A short, valid prefix of the real object so the resume path appends to it.
	srcObj, err := os.ReadFile(OpenRepo(srcRepo).objectPath(relevantCsum, "filez"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(relevantPart, srcObj[:1024], 0o644); err != nil {
		t.Fatal(err)
	}

	res, err := r.Pull(context.Background(), PullOptions{Remote: RemoteConfig{BaseURL: srv.URL}, Ref: ref, Concurrency: 1})
	if err != nil {
		t.Fatalf("pull failed: %v", err)
	}
	if res.Commit != commit {
		t.Fatalf("commit %s want %s", res.Commit, commit)
	}
	if res.PartsRemoved != 1 {
		t.Errorf("PartsRemoved=%d, want 1 (the stale sidecar)", res.PartsRemoved)
	}
	if _, err := os.Stat(stalePart); !os.IsNotExist(err) {
		t.Errorf("stale .part sidecar %s was not removed", stalePart)
	}
	// The relevant sidecar is consumed by a successful fetch; the repo must be valid.
	out := run(t, "ostree", "--repo="+dest, "fsck")
	if !strings.Contains(out, "no errors found") {
		t.Errorf("fsck failed:\n%s", out)
	}
}

// TestPullKeepsRelevantPartsOnResume verifies that re-running the SAME commit
// keeps every leftover .part (nothing is stale), so PartsRemoved is zero.
func TestPullKeepsRelevantPartsOnResume(t *testing.T) {
	requireOstree(t)
	srcRepo, ref, commit := makeContentRepo(t)
	srv := httptest.NewServer(http.FileServer(http.Dir(srcRepo)))
	defer srv.Close()

	dest := filepath.Join(t.TempDir(), "dest")
	r := OpenRepo(dest)

	// Seed a partial sidecar for a real content object of this commit.
	tmpDir := filepath.Join(dest, "tmp")
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		t.Fatal(err)
	}
	csum := largestFileObject(t, OpenRepo(srcRepo), commit)
	srcObj, err := os.ReadFile(OpenRepo(srcRepo).objectPath(csum, "filez"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tmpDir, csum+partFileSuffix), srcObj[:1024], 0o644); err != nil {
		t.Fatal(err)
	}

	res, err := r.Pull(context.Background(), PullOptions{Remote: RemoteConfig{BaseURL: srv.URL}, Ref: ref, Concurrency: 1})
	if err != nil {
		t.Fatalf("pull failed: %v", err)
	}
	if res.PartsRemoved != 0 {
		t.Errorf("PartsRemoved=%d, want 0 (the sidecar is still relevant)", res.PartsRemoved)
	}
	if res.Commit != commit {
		t.Fatalf("commit %s want %s", res.Commit, commit)
	}
}

// TestPullPrunesStalePartsOnNoOpPull verifies that the end-of-pull prune runs
// on success even when no content is fetched: a stale sidecar seeded before a
// re-pull of an already-complete commit is removed. This is the case the old
// mid-pull, content-set-scoped cleanup missed (a client that never fetches
// anything new, e.g. after a rollback).
func TestPullPrunesStalePartsOnNoOpPull(t *testing.T) {
	requireOstree(t)
	srcRepo, ref, _ := makeContentRepo(t)
	srv := httptest.NewServer(http.FileServer(http.Dir(srcRepo)))
	defer srv.Close()

	dest := filepath.Join(t.TempDir(), "dest")
	r := OpenRepo(dest)
	opts := PullOptions{Remote: RemoteConfig{BaseURL: srv.URL}, Ref: ref, Concurrency: 1}
	if _, err := r.Pull(context.Background(), opts); err != nil {
		t.Fatal(err)
	}

	// Seed a stale sidecar for a commit this pull does not reference, then re-pull
	// the already-complete commit: nothing is fetched, but the prune still fires.
	staleCsum := "0000000000000000000000000000000000000000000000000000000000000001"
	stalePart := filepath.Join(dest, "tmp", staleCsum+partFileSuffix)
	if err := os.WriteFile(stalePart, []byte("leftover"), 0o644); err != nil {
		t.Fatal(err)
	}

	res, err := r.Pull(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	if res.ContentFetched != 0 {
		t.Errorf("expected a no-op pull, fetched %d content objects", res.ContentFetched)
	}
	if res.PartsRemoved != 1 {
		t.Errorf("PartsRemoved=%d, want 1 (the stale sidecar)", res.PartsRemoved)
	}
	if _, err := os.Stat(stalePart); !os.IsNotExist(err) {
		t.Errorf("stale .part sidecar %s was not pruned", stalePart)
	}
}

// largestFileObject returns the checksum of the largest regular-file content
// object reachable from the commit (the random blob).
func largestFileObject(t *testing.T, r *Repo, commit string) string {
	t.Helper()
	c, err := r.ReadCommit(commit)
	if err != nil {
		t.Fatal(err)
	}
	var best string
	var bestSize int64
	var walk func(tree string)
	walk = func(tree string) {
		entries, err := r.ReadDirTree(tree)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range entries {
			if e.IsDir {
				walk(e.Checksum)
				continue
			}
			fi, err := os.Stat(r.objectPath(e.Checksum, "filez"))
			if err == nil && fi.Size() > bestSize {
				bestSize = fi.Size()
				best = e.Checksum
			}
		}
	}
	walk(c.RootDirTree)
	if best == "" {
		t.Fatal("no file object found")
	}
	return best
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

// failOneServer serves an archive repo but returns HTTP 500 for every request
// to one chosen content object, so its fetch fails permanently.
type failOneServer struct {
	root string
	fail string // substring of the path that should hard-fail
}

func (s *failOneServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rel := strings.TrimPrefix(r.URL.Path, "/")
	if s.fail != "" && strings.Contains(rel, s.fail) {
		http.Error(w, "boom", http.StatusInternalServerError)
		return
	}
	full := filepath.Join(s.root, filepath.FromSlash(rel))
	data, err := os.ReadFile(full)
	if err != nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	http.ServeContent(w, r, filepath.Base(full), zeroTime, strings.NewReader(string(data)))
}

// TestPullContentFetchErrorNoDeadlock is a regression test for a semaphore
// acquire/release imbalance in fetchContentObjects: when one content fetch
// failed and cancelled the context, the acquire loop could take the ctx.Done()
// branch yet still launch a goroutine that released a slot it never acquired,
// deadlocking wg.Wait() ("all goroutines are asleep - deadlock"). A failing
// object must make Pull return the error promptly instead of hanging.
func TestPullContentFetchErrorNoDeadlock(t *testing.T) {
	requireOstree(t)

	// Build a repo with many small content objects so the fetch runs well past
	// the concurrency limit while one object hard-fails mid-batch.
	dir := t.TempDir()
	srcRepo := filepath.Join(dir, "repo")
	tree := filepath.Join(dir, "tree")
	if err := os.MkdirAll(filepath.Join(tree, "files"), 0o755); err != nil {
		t.Fatal(err)
	}
	run(t, "ostree", "--repo="+srcRepo, "init", "--mode=archive")
	for i := 0; i < 60; i++ {
		writeRandom(t, filepath.Join(tree, "files", "f"+itoa(i)), 4096)
	}
	commit := strings.TrimSpace(run(t, "ostree", "--repo="+srcRepo, "commit", "--branch=main",
		"--owner-uid=0", "--owner-gid=0", "--tree=dir="+tree))

	// Pick any content object's .filez to fail on.
	var failCsum string
	filepath.Walk(filepath.Join(srcRepo, "objects"), func(p string, fi os.FileInfo, err error) error {
		if err == nil && !fi.IsDir() && strings.HasSuffix(p, ".filez") && failCsum == "" {
			failCsum = filepath.Base(p)
		}
		return nil
	})
	if failCsum == "" {
		t.Fatal("no .filez content object found in repo")
	}

	srv := httptest.NewServer(&failOneServer{root: srcRepo, fail: failCsum})
	defer srv.Close()

	dest := t.TempDir()
	done := make(chan error, 1)
	go func() {
		_, err := OpenRepo(dest).Pull(context.Background(), PullOptions{
			Remote: RemoteConfig{BaseURL: srv.URL}, Commit: commit, Concurrency: 4,
		})
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected a fetch error, got nil")
		}
	case <-time.After(30 * time.Second):
		// Dump goroutines to make a re-introduced deadlock obvious.
		buf := make([]byte, 1<<16)
		n := runtime.Stack(buf, true)
		t.Fatalf("Pull did not return within 30s (deadlock?):\n%s", buf[:n])
	}
}
