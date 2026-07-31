//go:build linux

package ostree

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"github.com/foundriesio/ostreeuploader/pkg/gvariant"
)

// xattrOstreeMeta is the user xattr under which bare-user repos store a content
// object's (uuua(ayay)) file metadata.
const xattrOstreeMeta = "user.ostreemeta"

// ensureRepo creates the directory skeleton and config of a bare-user(-only)
// repo at r.path if it is not already initialized. The mode is r.initMode when
// set, else bare-user. It is idempotent.
func (r *Repo) ensureRepo() error {
	cfg := filepath.Join(r.path, "config")
	if _, err := os.Stat(cfg); err == nil {
		return nil // already initialized
	}
	for _, d := range []string{
		"objects", "refs/heads", "refs/remotes", "refs/mirrors", "state", "tmp",
	} {
		if err := os.MkdirAll(filepath.Join(r.path, d), 0o755); err != nil {
			return fmt.Errorf("init repo: %w", err)
		}
	}
	mode := r.initMode
	if mode == "" {
		mode = modeBareUser
	}
	conf := "[core]\nrepo_version=1\nmode=" + mode + "\n"
	if err := os.WriteFile(cfg, []byte(conf), 0o644); err != nil {
		return fmt.Errorf("write repo config: %w", err)
	}
	return nil
}

// hasObject reports whether the loose object <csum>.<ext> already exists.
func (r *Repo) hasObject(csum, ext string) bool {
	_, err := os.Stat(r.objectPath(csum, ext))
	return err == nil
}

// writeFileAtomic writes data to a fresh temp file in the repo's objects dir,
// applies prepare (e.g. to set an xattr) on the temp path while it is still
// hidden, then renames it into place at dst. The parent dir of dst is created
// if missing.
func (r *Repo) writeFileAtomic(dst string, data []byte, mode os.FileMode, prepare func(path string) error) error {
	return r.writeFileAtomicFrom(dst, bytes.NewReader(data), mode, nil, prepare)
}

// writeFileAtomicFrom is writeFileAtomic sourced from an io.Reader: it copies
// src into a hidden temp file through a small buffer (so an arbitrarily large
// body never lands in memory), fsyncs, runs the optional verify hook (e.g. a
// streamed-checksum check) and prepare hook, then renames into place at dst.
// If verify fails the temp file is discarded and nothing is published.
func (r *Repo) writeFileAtomicFrom(dst string, src io.Reader, mode os.FileMode, verify func() error, prepare func(path string) error) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(dst), ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() {
		tmp.Close()
		os.Remove(tmpName) // no-op if already renamed
	}()
	if _, err := io.Copy(tmp, src); err != nil {
		return wrapENOSPC(err)
	}
	if err := tmp.Chmod(mode); err != nil {
		return err
	}
	// fsync the data before the rename so a power failure cannot leave a renamed
	// but empty/torn object in the content-addressed store.
	if err := tmp.Sync(); err != nil {
		return wrapENOSPC(err)
	}
	if err := tmp.Close(); err != nil {
		return wrapENOSPC(err)
	}
	// Verify (e.g. checksum) only after the full body is on disk, before it is
	// made visible under its content-addressed name.
	if verify != nil {
		if err := verify(); err != nil {
			return err
		}
	}
	if prepare != nil {
		if err := prepare(tmpName); err != nil {
			return err
		}
	}
	if err := wrapENOSPC(os.Rename(tmpName, dst)); err != nil {
		return err
	}
	// fsync the parent directory so the rename itself survives a power failure.
	return syncDir(filepath.Dir(dst))
}

// syncDir flushes a directory entry change (e.g. a rename into it) to disk.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// writeMetadataObject writes a commit/dirtree/dirmeta object verbatim after
// verifying its checksum equals sha256(raw bytes). ext is "commit", "dirtree",
// "dirmeta" or "commitmeta".
func (r *Repo) writeMetadataObject(csum, ext string, raw []byte) error {
	if ext != "commitmeta" { // detached metadata is not content-addressed by its own bytes
		if got := hex.EncodeToString(sha256Sum(raw)); got != csum {
			return fmt.Errorf("%s object %s: checksum mismatch (got %s)", ext, csum, got)
		}
	}
	return r.writeFileAtomic(r.objectPath(csum, ext), raw, 0o644, nil)
}

// writeContentObject converts a parsed content object into a loose object in the
// destination repo's mode, after verifying its checksum:
//
//   - bare-user: regular file body (or symlink target + trailing NUL), with the
//     (uuua(ayay)) file metadata stored in the user.ostreemeta xattr.
//   - bare-user-only: a regular file whose physical mode is the canonical mode
//     and no xattr, or a real on-disk symlink. uid/gid and xattrs are dropped
//     (the format cannot store them); only the mode must be representable
//     (fit 0775), else an error is returned.
func (r *Repo) writeContentObject(csum string, h fileHeader, content []byte) error {
	if got := contentObjectID(h, content); got != csum {
		return fmt.Errorf("content object %s: checksum mismatch (got %s)", csum, got)
	}
	if r.repoMode() == modeBareUserOnly {
		return r.writeContentObjectBareUserOnly(csum, h, content)
	}
	return r.writeContentObjectBareUser(csum, h, content)
}

// writeContentObjectStream is a streaming variant of writeContentObject for a
// regular-file content object: it copies the uncompressed body from body into
// the destination .file through a small buffer (never holding the whole object
// in memory), verifying the content-object checksum on the fly. Symlinks (whose
// body lives in the header, not the stream) must go through writeContentObject.
//
// The mode-specific finalization mirrors writeContentObject: bare-user stores an
// on-disk 0644 file plus the user.ostreemeta xattr; bare-user-only stores the
// canonical permission bits physically and no xattr (validated first).
func (r *Repo) writeContentObjectStream(csum string, h fileHeader, body io.Reader) error {
	if h.isSymlink() {
		return fmt.Errorf("writeContentObjectStream: %s is a symlink", csum)
	}
	var (
		fileMode os.FileMode = 0o644
		prepare  func(path string) error
	)
	if r.repoMode() == modeBareUserOnly {
		if err := validateBareUserOnly(csum, h); err != nil {
			return err
		}
		// Physical mode carries the permission bits; no xattr in bare-user-only.
		fileMode = os.FileMode(h.mode & 0o777)
	} else {
		meta := gvariant.EncodeFileMeta(h.uid, h.gid, h.mode, h.xattrs)
		prepare = func(path string) error {
			if err := syscall.Setxattr(path, xattrOstreeMeta, meta, 0); err != nil {
				return fmt.Errorf("set %s: %w", xattrOstreeMeta, err)
			}
			return nil
		}
	}
	// Hash the header + streamed body so the checksum is verified without a second
	// pass over the content.
	sum := newContentHasher(h)
	verify := func() error {
		if got := hex.EncodeToString(sum.Sum(nil)); got != csum {
			return fmt.Errorf("content object %s: checksum mismatch (got %s)", csum, got)
		}
		return nil
	}
	return r.writeFileAtomicFrom(r.objectPath(csum, "file"), io.TeeReader(body, sum), fileMode, verify, prepare)
}

// writeContentObjectBareUser writes a .file with the metadata held in the
// user.ostreemeta xattr (symlink stored as target + trailing NUL).
func (r *Repo) writeContentObjectBareUser(csum string, h fileHeader, content []byte) error {
	var body []byte
	if h.isSymlink() {
		body = append([]byte(h.symlink), 0) // target + trailing NUL
	} else {
		body = content
	}
	meta := gvariant.EncodeFileMeta(h.uid, h.gid, h.mode, h.xattrs)
	setMeta := func(path string) error {
		if err := syscall.Setxattr(path, xattrOstreeMeta, meta, 0); err != nil {
			return fmt.Errorf("set %s: %w", xattrOstreeMeta, err)
		}
		return nil
	}
	return r.writeFileAtomic(r.objectPath(csum, "file"), body, 0o644, setMeta)
}

// writeContentObjectBareUserOnly writes a .file in bare-user-only mode: a real
// symlink, or a regular file whose physical permission bits carry the mode (no
// xattr). It enforces the bare-user-only representability constraints.
func (r *Repo) writeContentObjectBareUserOnly(csum string, h fileHeader, content []byte) error {
	if h.isSymlink() {
		// A real on-disk symlink; metadata is not stored (canonical uid/gid 0).
		dst := r.objectPath(csum, "file")
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			return err
		}
		// Symlink atomically via a temp name + rename.
		tmp := dst + ".tmp"
		_ = os.Remove(tmp)
		if err := os.Symlink(h.symlink, tmp); err != nil {
			return wrapENOSPC(err)
		}
		if err := os.Rename(tmp, dst); err != nil {
			os.Remove(tmp)
			return wrapENOSPC(err)
		}
		// fsync the parent directory so the symlink rename survives a power failure.
		return syncDir(filepath.Dir(dst))
	}
	if err := validateBareUserOnly(csum, h); err != nil {
		return err
	}
	// Physical mode carries the permission bits (libostree stores it verbatim
	// after validation); type bits are implicit in being a regular file.
	mode := os.FileMode(h.mode & 0o777)
	return r.writeFileAtomic(r.objectPath(csum, "file"), content, mode, nil)
}

// validateBareUserOnly rejects content a bare-user-only repo cannot represent in
// its mode bits: setuid/setgid/sticky or world-write (anything outside 0775).
// This mirrors libostree's _ostree_validate_bareuseronly_mode, which validates
// ONLY the mode. uid/gid and xattrs are not checked here: bare-user-only cannot
// store them, so (like libostree) the writer simply drops them — published
// rootfs content commonly has non-zero uid/gid, and libostree pulls it fine.
func validateBareUserOnly(csum string, h fileHeader) error {
	if bits := h.mode & 0o7777 &^ 0o775; bits != 0 {
		return fmt.Errorf("content object %s: bare-user-only invalid mode %#o (forbidden bits %#o)", csum, h.mode&0o7777, bits)
	}
	return nil
}

// commitPartialPath returns the repo path of a commit's partial marker.
func (r *Repo) commitPartialPath(commit string) string {
	return filepath.Join(r.path, "state", commit+".commitpartial")
}

// markCommitPartial creates (partial=true) or removes (partial=false) the
// state/<commit>.commitpartial marker. The marker signals that not all of the
// commit's objects are present yet, enabling a safe resume.
func (r *Repo) markCommitPartial(commit string, partial bool) error {
	p := r.commitPartialPath(commit)
	if partial {
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			return err
		}
		f, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			return err
		}
		return f.Close()
	}
	if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// isCommitPartial reports whether the commit is marked partial.
func (r *Repo) isCommitPartial(commit string) bool {
	_, err := os.Stat(r.commitPartialPath(commit))
	return err == nil
}

// writeRef writes refs/heads/<ref> with the commit checksum.
func (r *Repo) writeRef(ref, commit string) error {
	p := filepath.Join(r.path, "refs", "heads", filepath.FromSlash(ref))
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	return os.WriteFile(p, []byte(commit+"\n"), 0o644)
}

func sha256Sum(b []byte) []byte {
	s := sha256.Sum256(b)
	return s[:]
}
