//go:build linux

package ostree

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// PullPhase identifies which stage of a pull is running.
type PullPhase string

const (
	PhaseMetadata PullPhase = "metadata" // commit/dirtree/dirmeta objects
	PhaseContent  PullPhase = "content"  // .filez content objects (full pull)
	PhaseDelta    PullPhase = "delta"    // static-delta parts
)

// PullProgress is a snapshot of pull progress. ObjectsDone counts whole objects
// completed in the current phase; ObjectsTotal is the total to fetch in that
// phase (0 when unknown). BytesDownloaded is cumulative over the wire this run.
type PullProgress struct {
	Phase           PullPhase
	ObjectsDone     int
	ObjectsTotal    int
	BytesDownloaded uint64
	Commit          string
}

// ProgressFunc receives progress snapshots. It may be called concurrently and
// frequently, so implementations must be cheap and non-blocking. Optional.
type ProgressFunc func(PullProgress)

// PullOptions configures a pull. Exactly one of Commit or Ref identifies what to
// fetch; if both are empty Pull fails. When Ref is given, it is resolved against
// the remote's refs/heads/<ref>.
type PullOptions struct {
	Remote RemoteConfig
	Ref    string // e.g. "main" (resolved via remote refs/heads)
	Commit string // explicit commit checksum (takes precedence over Ref)
	// From is the current (base) commit. When set and UseDelta is true, Pull
	// tries the from->to static delta the remote publishes before falling back
	// to a full object pull.
	From string
	// UseDelta enables the static-delta fast path (default: enabled when From is
	// set). Set NoDelta to force a full object pull.
	NoDelta bool
	// Concurrency bounds parallel object downloads (metadata walk and content
	// fetch). Default 8 when unset.
	Concurrency int
	// Progress, if set, receives progress snapshots during the pull. Optional;
	// nil means no reporting.
	Progress ProgressFunc
}

// PullResult reports what a pull did.
type PullResult struct {
	Commit          string
	MetaFetched     int    // metadata objects downloaded
	ContentFetched  int    // content objects downloaded
	ObjectsSkipped  int    // objects already present (resume)
	BytesDownloaded uint64 // total bytes pulled over the wire this run
	UsedDelta       bool   // true if the static-delta fast path was used
}

// fetcher pairs a transport with the destination repo and accumulates stats.
type fetcher struct {
	t    transport
	repo *Repo

	mu    sync.Mutex
	stats PullResult

	// progress reporting (all read/written under mu)
	progress     ProgressFunc // optional
	curPhase     PullPhase    // phase attributed to progress events
	contentTotal int          // total objects in the current phase (0 if unknown)
	contentDone  int          // content/delta objects completed (fetched or already present)
	lastEmit     time.Time    // last throttled byte-progress emission
}

// emit reports the current progress snapshot to the callback. Caller must NOT
// hold f.mu; it snapshots under the lock and calls the callback outside it.
func (f *fetcher) emit() {
	f.mu.Lock()
	if f.progress == nil {
		f.mu.Unlock()
		return
	}
	p := PullProgress{
		Phase:           f.curPhase,
		ObjectsDone:     f.contentDoneLocked(),
		ObjectsTotal:    f.contentTotal,
		BytesDownloaded: f.stats.BytesDownloaded,
		Commit:          f.stats.Commit,
	}
	cb := f.progress
	f.mu.Unlock()
	cb(p)
}

// contentDoneLocked returns how many objects count as completed in the current
// phase. Caller must hold f.mu. For metadata it is metadata objects fetched;
// for content/delta it is content objects completed (fetched or already present).
func (f *fetcher) contentDoneLocked() int {
	if f.curPhase == PhaseMetadata {
		return f.stats.MetaFetched
	}
	return f.contentDone
}

// addBytes adds n bytes (from a content/delta object body) to the running total
// and emits a throttled snapshot so progress advances within a large object
// without a callback per read. Content/delta bytes are counted here only (not
// in addContent), so wrapping the download reader is the single byte source.
func (f *fetcher) addBytes(n uint64) {
	f.mu.Lock()
	f.stats.BytesDownloaded += n
	emit := f.progress != nil && time.Since(f.lastEmit) >= 100*time.Millisecond
	if emit {
		f.lastEmit = time.Now()
	}
	f.mu.Unlock()
	if emit {
		f.emit()
	}
}

// progressReader wraps a reader to report bytes read to the fetcher.
type progressReader struct {
	r io.Reader
	f *fetcher
}

func (p *progressReader) Read(b []byte) (int, error) {
	n, err := p.r.Read(b)
	if n > 0 {
		p.f.addBytes(uint64(n))
	}
	return n, err
}

// Pull fetches the commit (and every object it references) from the remote
// archive repo into the local bare-user repo, resuming any prior interrupted
// attempt at both the object and byte level. It is safe to re-run.
func (r *Repo) Pull(ctx context.Context, opts PullOptions) (*PullResult, error) {
	if err := r.ensureRepo(); err != nil {
		return nil, err
	}
	// Resolve the repo mode once now, before any concurrent content writes read
	// it, so the lazy cache is populated single-threaded.
	r.repoMode()

	t, err := newTransport(opts.Remote)
	if err != nil {
		return nil, err
	}
	f := &fetcher{t: t, repo: r, progress: opts.Progress, curPhase: PhaseMetadata}

	commit := opts.Commit
	if commit == "" {
		if opts.Ref == "" {
			return nil, fmt.Errorf("pull: neither Commit nor Ref given")
		}
		commit, err = f.resolveRemoteRef(ctx, opts.Ref)
		if err != nil {
			return nil, err
		}
	}
	if !hexCsumRe.MatchString(commit) {
		return nil, fmt.Errorf("pull: %q is not a commit checksum", commit)
	}
	f.stats.Commit = commit // so progress snapshots carry the commit

	// Mark the commit partial up front; cleared only once every referenced
	// object is present, so an interrupted pull resumes safely.
	if err := r.markCommitPartial(commit, true); err != nil {
		return nil, err
	}

	// 1. Commit object (+ optional detached commitmeta).
	if err := f.fetchMetadata(ctx, commit, "commit"); err != nil {
		return nil, fmt.Errorf("fetch commit: %w", err)
	}
	if err := f.fetchOptionalMetadata(ctx, commit, "commitmeta"); err != nil {
		return nil, err
	}
	c, err := r.ReadCommit(commit)
	if err != nil {
		return nil, err
	}

	// 2. Prefer the static-delta fast path when a base commit is given and the
	// remote publishes a from->to delta; otherwise fall back to a full pull.
	deltaDone := false
	if opts.From != "" && !opts.NoDelta {
		ok, err := f.tryDeltaPull(ctx, opts.From, commit)
		if err != nil {
			return nil, err
		}
		deltaDone = ok
		f.stats.UsedDelta = ok
	}

	if !deltaDone {
		// Full object pull: walk the tree, fetching metadata concurrently and
		// collecting the set of content objects to download.
		content := newCsumSet()
		walkCtx, walkCancel := context.WithCancel(ctx)
		wp := newWalkPool(pullConcurrency(opts.Concurrency), walkCancel)
		err := f.walkDirTree(walkCtx, c.RootDirTree, c.RootDirMeta, content, wp)
		walkCancel()
		if err != nil {
			return nil, err
		}
		// Fetch content objects concurrently (each resumable). The total is known
		// up front, so progress can report N/M objects.
		f.mu.Lock()
		f.curPhase = PhaseContent
		f.contentTotal = len(content.list())
		f.mu.Unlock()
		if err := f.fetchContentObjects(ctx, content.list(), opts.Concurrency); err != nil {
			return nil, err
		}
	}

	// 3. All objects present: clear the partial marker, then write the ref.
	if err := r.markCommitPartial(commit, false); err != nil {
		return nil, err
	}
	if opts.Ref != "" {
		if err := r.writeRef(opts.Ref, commit); err != nil {
			return nil, err
		}
	}

	f.stats.Commit = commit
	res := f.stats
	return &res, nil
}

// resolveRemoteRef fetches refs/heads/<ref> from the remote and returns the
// commit checksum.
func (f *fetcher) resolveRemoteRef(ctx context.Context, ref string) (string, error) {
	data, err := f.get(ctx, filepath.ToSlash(filepath.Join("refs", "heads", ref)))
	if err != nil {
		return "", fmt.Errorf("resolve remote ref %q: %w", ref, err)
	}
	csum := trimRef(data)
	if !hexCsumRe.MatchString(csum) {
		return "", fmt.Errorf("remote ref %q: %q is not a commit checksum", ref, csum)
	}
	return csum, nil
}

func trimRef(b []byte) string {
	s := string(b)
	for len(s) > 0 && (s[len(s)-1] == '\n' || s[len(s)-1] == '\r' || s[len(s)-1] == ' ') {
		s = s[:len(s)-1]
	}
	return s
}

// walkDirTree fetches a dirtree and its dirmeta (skipping any already present),
// records its file content objects, and recurses into subdirectories.
//
// Subdirectories are walked concurrently under a shared semaphore: the metadata
// tree of a large rootfs is hundreds of dirtree/dirmeta objects, and a strictly
// serial depth-first walk pays one network round-trip per object before content
// can start. Fanning the recursion out overlaps those round-trips. w bounds the
// total in-flight walk goroutines across the whole recursion.
func (f *fetcher) walkDirTree(ctx context.Context, treeCsum, metaCsum string, content *csumSet, w *walkPool) error {
	if err := f.fetchMetadata(ctx, metaCsum, "dirmeta"); err != nil {
		return fmt.Errorf("fetch dirmeta %s: %w", metaCsum, err)
	}
	if err := f.fetchMetadata(ctx, treeCsum, "dirtree"); err != nil {
		return fmt.Errorf("fetch dirtree %s: %w", treeCsum, err)
	}
	entries, err := f.repo.ReadDirTree(treeCsum)
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	for _, e := range entries {
		if !e.IsDir {
			content.add(e.Checksum)
			continue
		}
		e := e
		// Try to run this subtree on a pool slot; if none is free, walk it inline
		// so a deep tree cannot deadlock waiting on itself for a slot.
		if w.acquire(ctx) {
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer w.release()
				if err := f.walkDirTree(ctx, e.Checksum, e.MetaSum, content, w); err != nil {
					w.setErr(err)
				}
			}()
		} else {
			if err := f.walkDirTree(ctx, e.Checksum, e.MetaSum, content, w); err != nil {
				w.setErr(err)
			}
		}
	}
	wg.Wait()
	return w.err()
}

// walkPool bounds the number of concurrent subtree-walk goroutines and captures
// the first error, cancelling the walk when one occurs.
type walkPool struct {
	sem    chan struct{}
	cancel context.CancelFunc
	mu     sync.Mutex
	first  error
}

func newWalkPool(concurrency int, cancel context.CancelFunc) *walkPool {
	if concurrency < 1 {
		concurrency = 1
	}
	return &walkPool{sem: make(chan struct{}, concurrency), cancel: cancel}
}

// acquire takes a pool slot without blocking; it returns false if none is free
// (caller then recurses inline) or the walk has been cancelled.
func (w *walkPool) acquire(ctx context.Context) bool {
	if ctx.Err() != nil {
		return false
	}
	select {
	case w.sem <- struct{}{}:
		return true
	default:
		return false
	}
}

func (w *walkPool) release() { <-w.sem }

func (w *walkPool) setErr(err error) {
	w.mu.Lock()
	if w.first == nil {
		w.first = err
		if w.cancel != nil {
			w.cancel()
		}
	}
	w.mu.Unlock()
}

func (w *walkPool) err() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.first
}

// fetchMetadata downloads a metadata object (small; no byte-resume needed) and
// writes it verbatim, unless it is already present.
func (f *fetcher) fetchMetadata(ctx context.Context, csum, ext string) error {
	if f.repo.hasObject(csum, ext) {
		f.addSkipped()
		return nil
	}
	data, err := f.get(ctx, objectRelPath(csum, ext))
	if err != nil {
		return err
	}
	if err := f.repo.writeMetadataObject(csum, ext, data); err != nil {
		return err
	}
	f.addMeta(uint64(len(data)))
	return nil
}

// fetchOptionalMetadata is like fetchMetadata but tolerates a missing object
// (e.g. detached commitmeta, which many commits lack).
func (f *fetcher) fetchOptionalMetadata(ctx context.Context, csum, ext string) error {
	err := f.fetchMetadata(ctx, csum, ext)
	if err != nil && isNotFound(err) {
		return nil
	}
	return err
}

// defaultConcurrency bounds parallel object downloads when the caller does not
// specify one. Higher than the old default of 4: with HTTP keep-alive pooling
// (see newHTTPRoundTripper) extra streams no longer cost a handshake each, and
// overlapping more small-object round-trips is the main lever on a high-latency
// remote.
const defaultConcurrency = 8

// pullConcurrency resolves the effective concurrency from an optional caller
// value (0 or negative means "use the default").
func pullConcurrency(c int) int {
	if c <= 0 {
		return defaultConcurrency
	}
	return c
}

// fetchContentObjects downloads each content object (resumably) and converts it
// to a bare-user .file object, with bounded concurrency.
func (f *fetcher) fetchContentObjects(ctx context.Context, csums []string, concurrency int) error {
	concurrency = pullConcurrency(concurrency)
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	var (
		errMu    sync.Mutex
		firstErr error
	)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for _, csum := range csums {
		csum := csum
		if f.repo.hasObject(csum, "file") {
			f.addContentSkipped()
			continue
		}
		// Acquire a slot, or stop launching work once the context is cancelled
		// (e.g. an earlier fetch failed). Taking the ctx.Done() branch must NOT
		// launch a goroutine: it never acquired a slot, so its release would
		// unbalance the semaphore and deadlock wg.Wait().
		select {
		case <-ctx.Done():
			// fall through to break out of the loop below
		case sem <- struct{}{}:
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { <-sem }()
				if err := f.fetchOneContent(ctx, csum); err != nil {
					errMu.Lock()
					if firstErr == nil {
						firstErr = err
						cancel()
					}
					errMu.Unlock()
				}
			}()
			continue
		}
		break
	}
	wg.Wait()
	if firstErr != nil {
		return firstErr
	}
	// The loop may have stopped early because the parent context was cancelled
	// without any fetch recording an error; surface that rather than a false ok.
	return ctx.Err()
}

// fetchOneContent resumably downloads a single .filez content object into a
// .part sidecar, then streams its inflated body straight into the bare-user
// object (verifying the checksum on the fly), so a large object is never held
// whole in memory.
func (f *fetcher) fetchOneContent(ctx context.Context, csum string) error {
	part := filepath.Join(f.repo.path, "tmp", csum+".filez.part")
	if err := os.MkdirAll(filepath.Dir(part), 0o755); err != nil {
		return err
	}
	if _, err := f.downloadResumable(ctx, objectRelPath(csum, "filez"), part); err != nil {
		return fmt.Errorf("download %s: %w", csum, err)
	}

	if err := f.writeContentFromPart(csum, part); err != nil {
		// A corrupt/incomplete part or checksum mismatch: drop it so a re-run
		// starts clean.
		os.Remove(part)
		return err
	}
	os.Remove(part)
	f.addContent()
	return nil
}

// writeContentFromPart parses the .filez header from the downloaded part file
// and writes the destination object. A regular file streams from disk through
// the inflate reader into the object (bounded memory); a symlink (empty body,
// target in the header) takes the small in-memory path.
func (f *fetcher) writeContentFromPart(csum, part string) error {
	pf, err := os.Open(part)
	if err != nil {
		return err
	}
	defer pf.Close()

	hdr, body, err := openFilez(pf)
	if err != nil {
		return fmt.Errorf("parse %s: %w", csum, err)
	}
	if body == nil { // symlink: body lives in the header
		if err := f.repo.writeContentObject(csum, hdr, nil); err != nil {
			return err
		}
		return nil
	}
	defer body.Close()
	if err := f.repo.writeContentObjectStream(csum, hdr, body); err != nil {
		return err
	}
	return nil
}

// downloadResumable downloads relPath into the part file, resuming from its
// current size when the server honors a Range request. It returns the number of
// bytes transferred during THIS call (i.e. excluding bytes already on disk from
// a previous attempt). When the server ignores Range (responds 200), the part
// is truncated and refetched from the start.
func (f *fetcher) downloadResumable(ctx context.Context, relPath, part string) (int64, error) {
	var offset int64
	if fi, err := os.Stat(part); err == nil {
		offset = fi.Size()
	}

	rc, resumed, err := f.t.open(ctx, relPath, offset)
	if err != nil {
		// A .part whose size is at/beyond the object length makes the server
		// reject the resume Range with 416 (e.g. a prior run finished the
		// download but was killed before the part was consumed). Recover by
		// discarding the stale part and refetching the whole object.
		if offset > 0 && isRangeNotSatisfiable(err) {
			rc, resumed, err = f.t.open(ctx, relPath, 0)
		}
		if err != nil {
			return 0, err
		}
	}
	defer rc.Close()

	flags := os.O_CREATE | os.O_WRONLY
	if resumed {
		flags |= os.O_APPEND // continue after the existing bytes
	} else {
		flags |= os.O_TRUNC // whole object (fresh, or server ignored Range)
	}
	out, err := os.OpenFile(part, flags, 0o644)
	if err != nil {
		return 0, err
	}
	defer out.Close()

	n, err := io.Copy(out, &progressReader{r: rc, f: f})
	if err != nil {
		return n, err // partial bytes are kept on disk for the next resume
	}
	return n, nil
}

// get downloads a whole small object into memory (used for metadata and refs).
func (f *fetcher) get(ctx context.Context, relPath string) ([]byte, error) {
	rc, _, err := f.t.open(ctx, relPath, 0)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(rc)
}

// --- stats helpers (concurrency-safe) ---

func (f *fetcher) addMeta(n uint64) {
	f.mu.Lock()
	f.stats.MetaFetched++
	f.stats.BytesDownloaded += n
	f.mu.Unlock()
	f.emit()
}

// addContent records a completed content object. Its bytes were already counted
// live through the wrapped download reader (addBytes), so only the count is
// incremented here.
func (f *fetcher) addContent() {
	f.mu.Lock()
	f.stats.ContentFetched++
	f.contentDone++
	f.mu.Unlock()
	f.emit()
}

// addSkipped records a metadata object that is already present (resume). It does
// not advance content-phase completion.
func (f *fetcher) addSkipped() {
	f.mu.Lock()
	f.stats.ObjectsSkipped++
	f.mu.Unlock()
	f.emit()
}

// addContentSkipped records a content object already present on disk: it counts
// as both a skipped object and a completed content-phase object.
func (f *fetcher) addContentSkipped() {
	f.mu.Lock()
	f.stats.ObjectsSkipped++
	f.contentDone++
	f.mu.Unlock()
	f.emit()
}

// csumSet is a small ordered, deduplicated, concurrency-safe set of checksums.
type csumSet struct {
	mu    sync.Mutex
	seen  map[string]bool
	order []string
}

func newCsumSet() *csumSet { return &csumSet{seen: map[string]bool{}} }

func (s *csumSet) add(c string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.seen[c] {
		s.seen[c] = true
		s.order = append(s.order, c)
	}
}

func (s *csumSet) list() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.order
}
