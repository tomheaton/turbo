package cache

import (
	"archive/tar"
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	log "log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"

	"github.com/DataDog/zstd"

	"github.com/vercel/turbo/cli/internal/turbopath"
)

func (cache *HttpCache) retrieve(hash string) (bool, []turbopath.AnchoredSystemPath, int, error) {
	resp, err := cache.client.FetchArtifact(hash)
	if err != nil {
		return false, nil, 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return false, nil, 0, nil // doesn't exist - not an error
	} else if resp.StatusCode != http.StatusOK {
		b, _ := ioutil.ReadAll(resp.Body)
		return false, nil, 0, fmt.Errorf("%s", string(b))
	}
	// If present, extract the duration from the response.
	duration := 0
	if resp.Header.Get("x-artifact-duration") != "" {
		intVar, err := strconv.Atoi(resp.Header.Get("x-artifact-duration"))
		if err != nil {
			return false, nil, 0, fmt.Errorf("invalid x-artifact-duration header: %w", err)
		}
		duration = intVar
	}
	var tarReader io.Reader

	defer func() { _ = resp.Body.Close() }()
	if cache.signerVerifier.isEnabled() {
		expectedTag := resp.Header.Get("x-artifact-tag")
		if expectedTag == "" {
			// If the verifier is enabled all incoming artifact downloads must have a signature
			return false, nil, 0, errors.New("artifact verification failed: Downloaded artifact is missing required x-artifact-tag header")
		}
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return false, nil, 0, fmt.Errorf("artifact verification failed: %w", err)
		}
		isValid, err := cache.signerVerifier.validate(hash, b, []byte(expectedTag))
		if err != nil {
			return false, nil, 0, fmt.Errorf("artifact verification failed: %w", err)
		}
		if !isValid {
			err = fmt.Errorf("artifact verification failed: artifact tag does not match expected tag %s", expectedTag)
			return false, nil, 0, err
		}
		// The artifact has been verified and the body can be read and untarred
		tarReader = bytes.NewReader(b)
	} else {
		tarReader = resp.Body
	}
	files, err := restoreTar(cache.repoRoot, tarReader)
	if err != nil {
		return false, nil, 0, err
	}
	return true, files, duration, nil
}

// restoreTar returns posix-style repo-relative paths of the files it
// restored. In the future, these should likely be repo-relative system paths
// so that they are suitable for being fed into cache.Put for other caches.
// For now, I think this is working because windows also accepts /-delimited paths.
func restoreTar(root turbopath.AbsoluteSystemPath, reader io.Reader) ([]turbopath.AnchoredSystemPath, error) {
	files := []turbopath.AnchoredSystemPath{}
	missingLinks := []*tar.Header{}
	zr := zstd.NewReader(reader)
	var closeError error
	defer func() { closeError = zr.Close() }()
	tr := tar.NewReader(zr)
	for {
		hdr, err := tr.Next()
		if err != nil {
			if err == io.EOF {
				for _, link := range missingLinks {
					err := restoreSymlink(root, link, true)
					if err != nil {
						return nil, err
					}
				}

				return files, closeError
			}
			return nil, err
		}
		// hdr.Name is always a posix-style path
		// FIXME: THIS IS A BUG.
		restoredName := turbopath.AnchoredUnixPath(hdr.Name)
		files = append(files, restoredName.ToSystemPath())
		filename := restoredName.ToSystemPath().RestoreAnchor(root)
		if isChild, err := root.ContainsPath(filename); err != nil {
			return nil, err
		} else if !isChild {
			return nil, fmt.Errorf("cannot untar file to %v", filename)
		}
		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := filename.MkdirAll(0775); err != nil {
				return nil, err
			}
		case tar.TypeReg:
			if dir := filename.Dir(); dir != "." {
				if err := dir.MkdirAll(0775); err != nil {
					return nil, err
				}
			}
			if f, err := filename.OpenFile(os.O_WRONLY|os.O_TRUNC|os.O_CREATE, os.FileMode(hdr.Mode)); err != nil {
				return nil, err
			} else if _, err := io.Copy(f, tr); err != nil {
				return nil, err
			} else if err := f.Close(); err != nil {
				return nil, err
			}
		case tar.TypeSymlink:
			if err := restoreSymlink(root, hdr, false); errors.Is(err, errNonexistentLinkTarget) {
				missingLinks = append(missingLinks, hdr)
			} else if err != nil {
				return nil, err
			}
		default:
			log.Printf("Unhandled file type %d for %s", hdr.Typeflag, hdr.Name)
		}
	}
}

var errNonexistentLinkTarget = errors.New("the link target does not exist")

func restoreSymlink(root turbopath.AbsoluteSystemPath, hdr *tar.Header, allowNonexistentTargets bool) error {
	// Note that hdr.Linkname is really the link target
	relativeLinkTarget := filepath.FromSlash(hdr.Linkname)
	linkFilename := root.UntypedJoin(hdr.Name)
	if err := linkFilename.EnsureDir(); err != nil {
		return err
	}

	// TODO: check if this is an absolute path, or if we even care
	linkTarget := linkFilename.Dir().UntypedJoin(relativeLinkTarget)
	if _, err := linkTarget.Lstat(); err != nil {
		if os.IsNotExist(err) {
			if !allowNonexistentTargets {
				return errNonexistentLinkTarget
			}
			// if we're allowing nonexistent link targets, proceed to creating the link
		} else {
			return err
		}
	}
	// Ensure that the link we're about to create doesn't already exist
	if err := linkFilename.Remove(); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := linkFilename.Symlink(relativeLinkTarget); err != nil {
		return err
	}
	return nil
}
