//go:build linux

package ostree

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
)

// TestOpenFilezStreamsContent verifies openFilez yields the same header and
// (streamed) uncompressed content as the in-memory parseFilez, for both a
// regular file and a symlink, over real .filez objects produced by ostree.
func TestOpenFilezStreamsContent(t *testing.T) {
	requireOstree(t)
	repo, _, to := makeContentRepo(t)
	r := OpenRepo(repo)

	var checkedFile, checkedLink bool
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
			data, err := os.ReadFile(r.objectPath(e.Checksum, "filez"))
			if err != nil {
				t.Fatal(err)
			}
			wantHdr, wantContent, err := parseFilez(data)
			if err != nil {
				t.Fatalf("parseFilez %s: %v", e.Name, err)
			}

			gotHdr, body, err := openFilez(bytes.NewReader(data))
			if err != nil {
				t.Fatalf("openFilez %s: %v", e.Name, err)
			}
			if gotHdr.uid != wantHdr.uid || gotHdr.gid != wantHdr.gid ||
				gotHdr.mode != wantHdr.mode || gotHdr.symlink != wantHdr.symlink {
				t.Errorf("%s: header mismatch stream=%+v mem=%+v", e.Name, gotHdr, wantHdr)
			}
			if wantHdr.isSymlink() {
				if body != nil {
					t.Errorf("%s: symlink should yield nil body reader", e.Name)
				}
				checkedLink = true
				continue
			}
			got, err := io.ReadAll(body)
			body.Close()
			if err != nil {
				t.Fatalf("%s: read streamed body: %v", e.Name, err)
			}
			if !bytes.Equal(got, wantContent) {
				t.Errorf("%s: streamed content != in-memory content (%d vs %d bytes)",
					e.Name, len(got), len(wantContent))
			}
			checkedFile = true
		}
	}
	walk(mustRootTree(t, r, to))
	if !checkedFile || !checkedLink {
		t.Fatalf("did not exercise both a file (%v) and a symlink (%v)", checkedFile, checkedLink)
	}
}

// TestWriteContentStreamChecksumMismatch verifies the streaming writer verifies
// the checksum after the body is on disk and, on mismatch, publishes no object
// (nothing is left under its content-addressed name).
func TestWriteContentStreamChecksumMismatch(t *testing.T) {
	requireOstree(t)
	repo, _, to := makeContentRepo(t)
	src := OpenRepo(repo)

	// The largest regular-file object (the random blob).
	csum := largestFileObject(t, src, to)
	data, err := os.ReadFile(src.objectPath(csum, "filez"))
	if err != nil {
		t.Fatal(err)
	}
	hdr, body, err := openFilez(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if body == nil {
		t.Fatal("expected a regular-file object, got a symlink")
	}
	// Corrupt the uncompressed stream by flipping bytes as they are read, so the
	// on-the-fly checksum will not match csum.
	corrupt := &flipReader{r: body}

	dest := OpenRepo(filepath.Join(t.TempDir(), "dest"))
	if err := dest.ensureRepo(); err != nil {
		t.Fatal(err)
	}
	err = dest.writeContentObjectStream(csum, hdr, corrupt)
	body.Close()
	if err == nil {
		t.Fatal("expected a checksum mismatch error, got nil")
	}
	if dest.hasObject(csum, "file") {
		t.Fatal("a corrupt object was published under its content-addressed name")
	}
	// No leftover temp files in the object dir either.
	entries, _ := os.ReadDir(filepath.Dir(dest.objectPath(csum, "file")))
	for _, de := range entries {
		if len(de.Name()) > 0 && de.Name()[0] == '.' {
			t.Errorf("leftover temp file after failed write: %s", de.Name())
		}
	}
}

// flipReader corrupts the first byte of the stream, then passes the rest
// through unchanged, so the streamed checksum differs from the original.
type flipReader struct {
	r       io.Reader
	flipped bool
}

func (f *flipReader) Read(b []byte) (int, error) {
	n, err := f.r.Read(b)
	if n > 0 && !f.flipped {
		b[0] ^= 0xff
		f.flipped = true
	}
	return n, err
}
