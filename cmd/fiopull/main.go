//go:build linux

// Command fiopull is a small CLI over the ostree package: a pure-Go ostree
// client (no libostree, no `ostree` binary). It is designed to be shelled out to
// by other tools (e.g. aktualizr-lite), hence the machine-readable `--format
// json` mode.
//
// fiopull is Device-Gateway-agnostic: it fetches ostree data from a plain
// HTTP(s) server (typically a signed object-store / GCS URL that the caller
// obtained from the gateway) or a local file:// repo. It does not speak the
// gateway download-urls protocol and has no mTLS/PKCS#11 handling; any auth is
// carried by the URL itself or via repeatable --header options. Subcommands:
//
//	update-size — the update's download/on-disk size, choosing the cheapest
//	              accurate source (static delta, else commit ostree.sizes
//	              metadata).
//	pull        — pull a commit (delta-aware, resumable) into a local repo.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/mattn/go-isatty"

	"github.com/foundriesio/ostreeuploader/pkg/ostree"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "update-size":
		os.Exit(cmdUpdateSize(os.Args[2:]))
	case "pull":
		os.Exit(cmdPull(os.Args[2:]))
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage:
  fiopull update-size --url URL (--ref REF | --commit CSUM) [--from CSUM]
                      [--header 'K: V' ...] [--format text|json]
  fiopull pull        [--from CSUM] [--no-delta] [--jobs N] [--header 'K: V' ...]
                      --repo PATH URL (COMMIT | REF)

URL may be an http(s):// or file:// base URL of an ostree repo (e.g. a signed
object-store URL obtained from the Device Gateway). fiopull does not talk to the
gateway itself; pass any auth via the URL or --header 'Authorization: Bearer ...'.

For "pull", URL and COMMIT-or-REF are mandatory positional arguments and must
come after any flags. A 64-char hex argument is treated as a commit checksum,
otherwise as a ref to resolve (e.g. main).

update-size picks the cheapest accurate source automatically: static delta if
one is published for --from->target, else the commit's ostree.sizes metadata.
Both are a single fetch. When neither is available it exits 4 so a caller can
decide what to do.
Exit codes: 0 ok, 1 error, 2 usage, 3 insufficient storage, 4 size unavailable.`)
}

func cmdUpdateSize(args []string) int {
	fs := flag.NewFlagSet("update-size", flag.ExitOnError)
	rurl := fs.String("url", "", "remote base URL (http(s):// or file://)")
	from := fs.String("from", "", "current/base commit; enables the static-delta fast path")
	ref := fs.String("ref", "", "ref to size (e.g. main)")
	commit := fs.String("commit", "", "explicit target commit checksum to size")
	repo := fs.String("repo", "", "path to local bare-user ostree repo; objects already present are excluded from the size")
	format := fs.String("format", "text", "output format: text|json")
	headers := headerFlags{}
	fs.Var(headers, "header", "extra request header 'Key: Value' (repeatable)")
	_ = fs.Parse(args)

	if *rurl == "" || (*ref == "" && *commit == "") {
		usage()
		return 2
	}

	rc := ostree.RemoteConfig{BaseURL: *rurl, Headers: headers}

	var localRepo *ostree.Repo
	if *repo != "" {
		localRepo = ostree.OpenRepo(*repo)
	}

	ctx := context.Background()
	to := *commit
	if to == "" {
		var err error
		if to, err = ostree.ResolveRemoteRef(ctx, rc, *ref); err != nil {
			return fail(*format, err)
		}
	}

	us, err := ostree.RemoteUpdateSize(ctx, rc, *from, to, localRepo)
	if err != nil {
		if errors.Is(err, ostree.ErrSizeUnavailable) {
			// No efficient estimate available (no static delta, no ostree.sizes).
			// Distinct exit code so a caller can proceed without a size rather
			// than treat this as a hard failure.
			if *format == "json" {
				emitJSON(map[string]string{"error": err.Error(), "method": "unavailable"})
			} else {
				fmt.Fprintln(os.Stderr, "size unavailable:", err)
			}
			return 4
		}
		return fail(*format, err)
	}
	if *format == "json" {
		emitJSON(us)
	} else {
		fmt.Printf("Commit: %s\n", to)
		fmt.Printf("Download Size: %d (%s)\n", us.Compressed, ostree.FormatBytes(us.Compressed))
		if us.Uncompressed > 0 {
			fmt.Printf("Uncompressed Size: %d (%s)\n", us.Uncompressed, ostree.FormatBytes(us.Uncompressed))
		}
		fmt.Printf("Method: %s\n", us.Method)
	}
	return 0
}

// headerFlags collects repeatable --header 'K: V' options.
type headerFlags map[string]string

func (h headerFlags) String() string { return "" }

func (h headerFlags) Set(v string) error {
	i := strings.IndexByte(v, ':')
	if i < 0 {
		return fmt.Errorf("header must be 'Key: Value', got %q", v)
	}
	key := strings.TrimSpace(v[:i])
	val := strings.TrimSpace(v[i+1:])
	if key == "" {
		return fmt.Errorf("empty header key in %q", v)
	}
	h[key] = val
	return nil
}

func cmdPull(args []string) int {
	fs := flag.NewFlagSet("pull", flag.ExitOnError)
	repo := fs.String("repo", "", "path to local bare-user ostree repo (created if absent)")
	from := fs.String("from", "", "current/base commit; enables the static-delta fast path")
	noDelta := fs.Bool("no-delta", false, "force a full object pull (disable static deltas)")
	jobs := fs.Int("jobs", 8, "concurrent content downloads")
	progress := fs.String("progress", "auto", "progress reporting: auto (TTY bar, else silent), log (periodic lines to stderr, for non-interactive callers), none")
	headers := headerFlags{}
	fs.Var(headers, "header", "extra request header 'Key: Value' (repeatable)")
	_ = fs.Parse(args)

	// URL and the commit/ref are mandatory positional arguments (flags, if any,
	// must precede them). The second positional is treated as a commit checksum
	// when it looks like one, otherwise as a ref to resolve.
	if fs.NArg() != 2 || *repo == "" {
		usage()
		return 2
	}
	rurl := fs.Arg(0)
	commit, ref := "", ""
	if looksLikeCommit(fs.Arg(1)) {
		commit = fs.Arg(1)
	} else {
		ref = fs.Arg(1)
	}

	rc := ostree.RemoteConfig{BaseURL: rurl, Headers: headers}

	opts := ostree.PullOptions{
		Remote:      rc,
		Ref:         ref,
		Commit:      commit,
		From:        *from,
		NoDelta:     *noDelta,
		Concurrency: *jobs,
	}
	// Select a progress reporter. "auto" renders a \r bar only when stderr is a
	// terminal and is otherwise silent (so machine callers and CI logs are not
	// polluted with \r); "log" emits periodic newline-terminated lines suitable
	// for a non-interactive parent that captures stderr line by line (e.g.
	// aktualizr-lite); "none" disables reporting.
	var finish func()
	switch *progress {
	case "auto":
		if isatty.IsTerminal(os.Stderr.Fd()) {
			opts.Progress = newTTYProgress()
			finish = finishTTYProgress
		}
	case "log":
		opts.Progress = newLogProgress()
	case "none":
		// no reporting
	default:
		fmt.Fprintf(os.Stderr, "invalid --progress %q (want auto|log|none)\n", *progress)
		return 2
	}

	res, err := ostree.OpenRepo(*repo).Pull(context.Background(), opts)
	if finish != nil {
		finish()
	}
	if err != nil {
		if errors.Is(err, ostree.ErrInsufficientStorage) {
			fmt.Fprintln(os.Stderr, "INSUFFICIENT STORAGE:", err)
			return 3
		}
		return fail("text", err)
	}
	mode := "full"
	if res.UsedDelta {
		mode = "delta"
	}
	fmt.Printf("pulled commit %s (%s)\n", res.Commit, mode)
	fmt.Printf("  metadata fetched: %d\n", res.MetaFetched)
	fmt.Printf("  content fetched:  %d\n", res.ContentFetched)
	fmt.Printf("  skipped (resume): %d\n", res.ObjectsSkipped)
	fmt.Printf("  bytes downloaded: %d (%s)\n", res.BytesDownloaded, ostree.FormatBytes(res.BytesDownloaded))
	return 0
}

// looksLikeCommit reports whether s is a 64-char lowercase hex ostree commit
// checksum. Anything else is treated as a ref name to resolve.
func looksLikeCommit(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, c := range s {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

func emitJSON(v any) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func fail(format string, err error) int {
	if format == "json" {
		emitJSON(map[string]string{"error": err.Error()})
	} else {
		fmt.Fprintln(os.Stderr, "error:", err)
	}
	return 1
}
