use std::{backtrace::Backtrace, env::current_dir, fs, os, path::Path};

use log::{debug, error, info};
use tar::{Archive, EntryType, Header};
use turbopath::{AbsoluteSystemPathBuf, AnchoredSystemPathBuf, RelativeUnixPathBuf};
use turborepo_api_client::APIClient;

use crate::{signature_authentication::ArtifactSignatureAuthenticator, CacheError};

pub struct HttpCache {
    client: APIClient,
    signer_verifier: Option<ArtifactSignatureAuthenticator>,
    repo_root: AbsoluteSystemPathBuf,
}

impl HttpCache {
    pub fn new(
        client: APIClient,
        signer_verifier: Option<ArtifactSignatureAuthenticator>,
        repo_root: AbsoluteSystemPathBuf,
    ) -> HttpCache {
        HttpCache {
            client,
            signer_verifier,
            repo_root,
        }
    }

    pub async fn retrieve(
        &self,
        hash: &str,
        token: &str,
        team_id: &str,
        team_slug: Option<&str>,
        use_preflight: bool,
    ) -> Result<(Vec<AnchoredSystemPathBuf>, u64), CacheError> {
        let response = self
            .client
            .fetch_artifact(hash, token, team_id, team_slug, use_preflight)
            .await?;

        let duration = if let Some(duration) = response.headers().get("x-artifact-duration") {
            let duration = duration
                .to_str()
                .map_err(|_| CacheError::InvalidDuration(Backtrace::capture()))?;
            duration
                .parse::<u64>()
                .map_err(|_| CacheError::InvalidDuration(Backtrace::capture()))?
        } else {
            0
        };

        let body = if let Some(signer_verifier) = &self.signer_verifier {
            let expected_tag = response
                .headers()
                .get("x-artifact-tag")
                .ok_or(CacheError::ArtifactTagMissing(Backtrace::capture()))?;

            let expected_tag = expected_tag
                .to_str()
                .map_err(|_| CacheError::InvalidTag(Backtrace::capture()))?
                .to_string();

            let body = response.bytes().await.map_err(|e| {
                CacheError::ApiClientError(
                    turborepo_api_client::Error::ReqwestError(e),
                    Backtrace::capture(),
                )
            })?;
            let is_valid = signer_verifier.validate(hash, &body, &expected_tag)?;

            if !is_valid {
                return Err(CacheError::InvalidTag(Backtrace::capture()));
            }

            body
        } else {
            response.bytes().await.map_err(|e| {
                CacheError::ApiClientError(
                    turborepo_api_client::Error::ReqwestError(e),
                    Backtrace::capture(),
                )
            })?
        };

        let files = Self::restore_tar(&self.repo_root, &body)?;

        Ok((files, duration))
    }

    fn set_dir_mode(mode: u32, path: impl AsRef<Path>) -> Result<(), CacheError> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let metadata = fs::metadata(&path)?;
            let mut permissions = metadata.permissions();
            permissions.set_mode(mode);

            fs::set_permissions(path, permissions)?;
        }

        Ok(())
    }

    pub(crate) fn restore_tar(
        root: &AbsoluteSystemPathBuf,
        body: &[u8],
    ) -> Result<Vec<AnchoredSystemPathBuf>, CacheError> {
        let mut files = Vec::new();
        let mut missing_links = Vec::new();
        let tar_reader = zstd::Decoder::new(&body[..])?;
        let mut tr = Archive::new(tar_reader);

        for entry in tr.entries()? {
            let mut entry = entry?;
            let restored_name = RelativeUnixPathBuf::new(entry.path()?)?;
            let restored_anchored_path: AnchoredSystemPathBuf =
                <RelativeUnixPathBuf as TryInto<_>>::try_into(restored_name)?;
            let filename = root.resolve(&restored_anchored_path);
            files.push(restored_anchored_path.clone());

            let is_child = filename.starts_with(root);
            if !is_child {
                return Err(CacheError::InvalidFilePath(
                    filename.to_string_lossy().to_string(),
                    Backtrace::capture(),
                ));
            }
            let header = entry.header();
            match header.entry_type() {
                EntryType::Directory => {
                    info!("Restoring directory {}", filename.to_string_lossy());
                    fs::create_dir_all(&filename)?;
                    Self::set_dir_mode(0o775, &filename)?;
                }
                EntryType::Regular => {
                    info!("Restoring file {}", filename.to_string_lossy());

                    if let Some(parent) = filename.parent() {
                        if parent.as_ref() != current_dir()?.as_path() {
                            fs::create_dir_all(&parent)?;
                            Self::set_dir_mode(0o775, &parent)?;
                        }
                    }
                    entry.unpack(&filename)?;
                }
                EntryType::Symlink => {
                    info!("Restoring symlink {}", filename.to_string_lossy());

                    if let Err(CacheError::LinkTargetDoesNotExist(_, _)) =
                        Self::restore_symlink(root, header, false)
                    {
                        missing_links.push(header.clone());
                    }
                }
                entry_type => {
                    error!(
                        "Unhandled file type {:?} for {}",
                        entry_type,
                        filename.to_string_lossy()
                    )
                }
            }
        }

        for link in missing_links {
            info!(
                "Restoring missing symlink {}",
                link.path()?.to_string_lossy()
            );

            Self::restore_symlink(root, &link, true)?;
        }
        Ok(files)
    }

    fn restore_symlink(
        root: &AbsoluteSystemPathBuf,
        header: &Header,
        allow_nonexistent_targets: bool,
    ) -> Result<(), CacheError> {
        let link_file_path = header.path()?;
        let anchored_link_file_path = link_file_path.as_ref().try_into()?;

        let absolute_link_file_path = root.resolve(&anchored_link_file_path);
        fs::create_dir_all(absolute_link_file_path.parent().ok_or_else(|| {
            CacheError::InvalidFilePath(
                absolute_link_file_path.to_string_lossy().to_string(),
                Backtrace::capture(),
            )
        })?)?;

        // This is extra confusing for no reason. On some systems the link name is the
        // name of the link file, on tar it's the name of the link target.
        let anchored_link_target: AnchoredSystemPathBuf = header
            .link_name()?
            .ok_or_else(|| CacheError::LinkTargetNotOnHeader(Backtrace::capture()))?
            .as_ref()
            .try_into()?;

        let absolute_link_target = root.resolve(&anchored_link_target);
        if !absolute_link_target.exists() && !allow_nonexistent_targets {
            debug!(
                "Link target {} does not exist",
                absolute_link_target.to_string_lossy()
            );
            return Err(CacheError::LinkTargetDoesNotExist(
                absolute_link_target.to_string_lossy().to_string(),
                Backtrace::capture(),
            ));
        }

        if fs::symlink_metadata(&absolute_link_file_path).is_ok() {
            fs::remove_file(&absolute_link_file_path)?;
        }
        debug!(
            "Linking {} -> {}",
            absolute_link_file_path.to_string_lossy(),
            absolute_link_target.to_string_lossy()
        );
        #[cfg(unix)]
        os::unix::fs::symlink(&absolute_link_target, &absolute_link_file_path)?;
        println!(
            "{} is symlink: {}",
            absolute_link_file_path.to_string_lossy(),
            absolute_link_file_path.as_path().is_symlink()
        );
        #[cfg(windows)]
        os::windows::fs::symlink_file(&absolute_link_target, &absolute_link_file_path)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, fs, fs::File};

    use anyhow::Result;
    use log::debug;
    use tempfile::{tempdir, TempDir};
    use test_log::test;
    use turbopath::{AbsoluteSystemPathBuf, AnchoredSystemPathBuf, RelativeSystemPathBuf};

    use crate::http::HttpCache;

    #[derive(Debug)]
    struct ExpectedError {
        #[cfg(unix)]
        unix: String,
        #[cfg(windows)]
        windows: String,
    }

    // Expected output of the cache
    #[derive(Debug)]
    struct ExpectedOutput(Vec<AnchoredSystemPathBuf>);

    enum TarFile {
        File {
            body: Vec<u8>,
            path: AnchoredSystemPathBuf,
        },
        Directory {
            path: AnchoredSystemPathBuf,
        },
        Symlink {
            path: AnchoredSystemPathBuf,
            linkname: AnchoredSystemPathBuf,
        },
    }

    struct TestCase {
        name: &'static str,
        // The files we start with
        input_files: Vec<TarFile>,
        expected_error: Option<ExpectedError>,
        // The expected files (there will be more files than `expected_output`
        // since we want to check entries of symlinked directories)
        expected_files: Vec<TarFile>,
    }

    fn create_files(test_dir: &TempDir, files: &[TarFile]) -> Result<()> {
        for file in files {
            match file {
                TarFile::File { path, body } => {
                    let absolute_path = test_dir.path().join(path.as_path());
                    debug!("creating file: {:?}", absolute_path);
                    fs::write(absolute_path, body)?;
                }
                TarFile::Directory { path } => {
                    let absolute_path = test_dir.path().join(path.as_path());
                    debug!("creating directory: {:?}", absolute_path);
                    fs::create_dir(&absolute_path)?;
                    debug!("exists: {:?}", absolute_path.exists());
                }
                TarFile::Symlink { path, linkname } => {
                    let absolute_source = test_dir.path().join(path.as_path());
                    let absolute_linkname = test_dir.path().join(linkname.as_path());

                    debug!(
                        "creating symlink from {:?} to {:?}",
                        absolute_linkname, absolute_source
                    );

                    if fs::symlink_metadata(&absolute_linkname).is_ok() {
                        fs::remove_file(&absolute_linkname)?;
                    }
                    #[cfg(unix)]
                    std::os::unix::fs::symlink(&absolute_source, &absolute_linkname)?;
                    #[cfg(windows)]
                    std::os::windows::fs::symlink_file(&absolute_source, &absolute_linkname)?;
                }
            }
        }

        Ok(())
    }

    fn generate_tar(test_dir: &TempDir, files: &[TarFile]) -> Result<AbsoluteSystemPathBuf> {
        let test_archive_path = test_dir.path().join("test.tar");
        let archive_file = File::create(&test_archive_path)?;

        let mut tar_writer = tar::Builder::new(archive_file);

        for file in files {
            match file {
                TarFile::File { path, body: _ } => {
                    debug!("Adding file: {:?}", path);
                    let absolute_path = test_dir.path().join(&path);
                    tar_writer.append_path_with_name(absolute_path, path)?;
                }
                TarFile::Directory { path } => {
                    debug!("Adding directory: {:?}", path);
                    let absolute_path = test_dir.path().join(&path);

                    tar_writer.append_dir(&path, absolute_path)?;
                }
                TarFile::Symlink { path, linkname } => {
                    debug!("Adding symlink: {:?} -> {:?}", linkname, path);
                    let mut header = tar::Header::new_gnu();
                    header.set_username("foo")?;
                    header.set_entry_type(tar::EntryType::Symlink);
                    header.set_size(0);

                    tar_writer.append_link(&mut header, &linkname, &path)?;
                }
            }
        }

        tar_writer.into_inner()?;

        Ok(AbsoluteSystemPathBuf::new(test_archive_path)?)
    }

    fn compress_tar(archive_path: &AbsoluteSystemPathBuf) -> Result<AbsoluteSystemPathBuf> {
        let mut input_file = File::open(archive_path)?;

        let output_file_path = format!("{}.zst", archive_path.to_str()?);
        let output_file = std::fs::File::create(&output_file_path)?;

        let mut zw = zstd::stream::Encoder::new(output_file, 0)?;
        std::io::copy(&mut input_file, &mut zw)?;

        zw.finish()?;

        Ok(AbsoluteSystemPathBuf::new(output_file_path)?)
    }

    fn assert_file_exists(anchor: &AbsoluteSystemPathBuf, disk_file: &TarFile) -> Result<()> {
        match disk_file {
            TarFile::File { path, body } => {
                let full_name = anchor.resolve(path.into());
                debug!("reading {}", full_name.to_string_lossy());
                let file_contents = fs::read(full_name)?;

                assert_eq!(file_contents, *body);
            }
            TarFile::Directory { path } => {
                let full_name = anchor.resolve(path.into());
                let metadata = fs::metadata(full_name)?;

                assert!(metadata.is_dir());
            }
            TarFile::Symlink {
                path: expected_path,
                linkname,
            } => {
                let full_linkname = anchor.resolve(linkname.into());
                let full_path = fs::read_link(full_linkname)?;
                let full_expected_path = anchor.resolve(expected_path.into());
                assert_eq!(full_path, full_expected_path.to_path_buf());
            }
        }

        Ok(())
    }

    #[test]
    fn test_open() -> Result<()> {
        let tests = vec![
            TestCase {
                name: "cache optimized",
                input_files: vec![
                    TarFile::Directory {
                        path: RelativeSystemPathBuf::new("one/")?.into(),
                    },
                    TarFile::Directory {
                        path: RelativeSystemPathBuf::new("one/two/")?.into(),
                    },
                    TarFile::Directory {
                        path: RelativeSystemPathBuf::new("one/two/three/")?.into(),
                    },
                    TarFile::Directory {
                        path: RelativeSystemPathBuf::new("one/two/a/")?.into(),
                    },
                    TarFile::File {
                        body: vec![],
                        path: RelativeSystemPathBuf::new("one/two/three/file-one")?.into(),
                    },
                    TarFile::File {
                        body: vec![],
                        path: RelativeSystemPathBuf::new("one/two/three/file-two")?.into(),
                    },
                    TarFile::File {
                        body: vec![],
                        path: RelativeSystemPathBuf::new("one/two/a/file")?.into(),
                    },
                    TarFile::Directory {
                        path: RelativeSystemPathBuf::new("one/two/b/")?.into(),
                    },
                    TarFile::File {
                        body: vec![],
                        path: RelativeSystemPathBuf::new("one/two/b/file")?.into(),
                    },
                ],
                expected_files: vec![
                    TarFile::Directory {
                        path: RelativeSystemPathBuf::new("one/")?.into(),
                    },
                    TarFile::Directory {
                        path: RelativeSystemPathBuf::new("one/two/")?.into(),
                    },
                    TarFile::Directory {
                        path: RelativeSystemPathBuf::new("one/two/three/")?.into(),
                    },
                    TarFile::Directory {
                        path: RelativeSystemPathBuf::new("one/two/a/")?.into(),
                    },
                    TarFile::File {
                        path: RelativeSystemPathBuf::new("one/two/three/file-one")?.into(),
                        body: vec![],
                    },
                    TarFile::File {
                        path: RelativeSystemPathBuf::new("one/two/three/file-two")?.into(),
                        body: vec![],
                    },
                    TarFile::File {
                        path: RelativeSystemPathBuf::new("one/two/a/file")?.into(),
                        body: vec![],
                    },
                    TarFile::Directory {
                        path: RelativeSystemPathBuf::new("one/two/b/")?.into(),
                    },
                    TarFile::File {
                        path: RelativeSystemPathBuf::new("one/two/b/file")?.into(),
                        body: vec![],
                    },
                ],
                expected_error: None,
            },
            TestCase {
                name: "pathological cache works",
                input_files: vec![
                    TarFile::Directory {
                        path: RelativeSystemPathBuf::new("one/")?.into(),
                    },
                    TarFile::Directory {
                        path: RelativeSystemPathBuf::new("one/two/")?.into(),
                    },
                    TarFile::Directory {
                        path: RelativeSystemPathBuf::new("one/two/a/")?.into(),
                    },
                    TarFile::Directory {
                        path: RelativeSystemPathBuf::new("one/two/b/")?.into(),
                    },
                    TarFile::Directory {
                        path: RelativeSystemPathBuf::new("one/two/three/")?.into(),
                    },
                    TarFile::File {
                        body: vec![],
                        path: RelativeSystemPathBuf::new("one/two/a/file")?.into(),
                    },
                    TarFile::File {
                        body: vec![],
                        path: RelativeSystemPathBuf::new("one/two/b/file")?.into(),
                    },
                    TarFile::File {
                        body: vec![],
                        path: RelativeSystemPathBuf::new("one/two/three/file-one")?.into(),
                    },
                    TarFile::File {
                        body: vec![],
                        path: RelativeSystemPathBuf::new("one/two/three/file-two")?.into(),
                    },
                ],
                expected_error: None,
                expected_files: vec![
                    TarFile::Directory {
                        path: RelativeSystemPathBuf::new("one/")?.into(),
                    },
                    TarFile::Directory {
                        path: RelativeSystemPathBuf::new("one/two/")?.into(),
                    },
                    TarFile::Directory {
                        path: RelativeSystemPathBuf::new("one/two/a/")?.into(),
                    },
                    TarFile::Directory {
                        path: RelativeSystemPathBuf::new("one/two/b/")?.into(),
                    },
                    TarFile::Directory {
                        path: RelativeSystemPathBuf::new("one/two/three/")?.into(),
                    },
                    TarFile::File {
                        path: RelativeSystemPathBuf::new("one/two/a/file")?.into(),
                        body: vec![],
                    },
                    TarFile::File {
                        path: RelativeSystemPathBuf::new("one/two/b/file")?.into(),
                        body: vec![],
                    },
                    TarFile::File {
                        path: RelativeSystemPathBuf::new("one/two/three/file-one")?.into(),
                        body: vec![],
                    },
                    TarFile::File {
                        path: RelativeSystemPathBuf::new("one/two/three/file-two")?.into(),
                        body: vec![],
                    },
                ],
            },
            TestCase {
                name: "symlink hello world",
                input_files: vec![
                    TarFile::Directory {
                        path: RelativeSystemPathBuf::new("target")?.into(),
                    },
                    TarFile::Symlink {
                        path: RelativeSystemPathBuf::new("target")?.into(),
                        linkname: RelativeSystemPathBuf::new("source")?.into(),
                    },
                ],
                expected_files: vec![
                    TarFile::Directory {
                        path: RelativeSystemPathBuf::new("target")?.into(),
                    },
                    TarFile::Symlink {
                        path: RelativeSystemPathBuf::new("target")?.into(),
                        linkname: RelativeSystemPathBuf::new("source")?.into(),
                    },
                ],
                expected_error: None,
            },
            TestCase {
                name: "nested file",
                input_files: vec![
                    TarFile::Directory {
                        path: RelativeSystemPathBuf::new("folder/")?.into(),
                    },
                    TarFile::File {
                        body: b"file".to_vec(),
                        path: RelativeSystemPathBuf::new("folder/file")?.into(),
                    },
                ],
                expected_files: vec![
                    TarFile::Directory {
                        path: RelativeSystemPathBuf::new("folder/")?.into(),
                    },
                    TarFile::File {
                        path: RelativeSystemPathBuf::new("folder/file")?.into(),
                        body: b"file".to_vec(),
                    },
                ],
                expected_error: None,
            },
            TestCase {
                name: "pathological symlinks",
                input_files: vec![
                    TarFile::File {
                        body: b"real".to_vec(),
                        path: RelativeSystemPathBuf::new("real")?.into(),
                    },
                    TarFile::Symlink {
                        path: RelativeSystemPathBuf::new("real")?.into(),
                        linkname: RelativeSystemPathBuf::new("one")?.into(),
                    },
                    TarFile::Symlink {
                        path: RelativeSystemPathBuf::new("one")?.into(),
                        linkname: RelativeSystemPathBuf::new("two")?.into(),
                    },
                    TarFile::Symlink {
                        path: RelativeSystemPathBuf::new("two")?.into(),
                        linkname: RelativeSystemPathBuf::new("three")?.into(),
                    },
                ],
                expected_error: None,
                expected_files: vec![
                    TarFile::File {
                        path: RelativeSystemPathBuf::new("real")?.into(),
                        body: b"real".to_vec(),
                    },
                    TarFile::Symlink {
                        path: RelativeSystemPathBuf::new("real")?.into(),
                        linkname: RelativeSystemPathBuf::new("one")?.into(),
                    },
                    TarFile::Symlink {
                        path: RelativeSystemPathBuf::new("one")?.into(),
                        linkname: RelativeSystemPathBuf::new("two")?.into(),
                    },
                    TarFile::Symlink {
                        path: RelativeSystemPathBuf::new("two")?.into(),
                        linkname: RelativeSystemPathBuf::new("three")?.into(),
                    },
                ],
            },
            TestCase {
                name: "symlink clobber",
                input_files: vec![
                    TarFile::File {
                        body: b"real".to_vec(),
                        path: RelativeSystemPathBuf::new("real")?.into(),
                    },
                    TarFile::Symlink {
                        linkname: RelativeSystemPathBuf::new("one")?.into(),
                        path: RelativeSystemPathBuf::new("real")?.into(),
                    },
                    TarFile::Symlink {
                        linkname: RelativeSystemPathBuf::new("one")?.into(),
                        path: RelativeSystemPathBuf::new("two")?.into(),
                    },
                    TarFile::Symlink {
                        linkname: RelativeSystemPathBuf::new("one")?.into(),
                        path: RelativeSystemPathBuf::new("three")?.into(),
                    },
                ],
                expected_files: vec![
                    TarFile::Symlink {
                        linkname: RelativeSystemPathBuf::new("one")?.into(),
                        path: RelativeSystemPathBuf::new("three")?.into(),
                    },
                    TarFile::File {
                        path: RelativeSystemPathBuf::new("real")?.into(),
                        body: b"real".to_vec(),
                    },
                ],
                expected_error: None,
            },
        ];
        for test in tests {
            debug!("\n\ntest: {}\n\n", test.name);
            let input_dir = tempdir()?;
            create_files(&input_dir, &test.input_files)?;
            let archive_path = generate_tar(&input_dir, &test.input_files)?;
            let compressed_archive_path = compress_tar(&archive_path)?;
            let output_dir = tempdir()?;
            let anchor = AbsoluteSystemPathBuf::new(output_dir.path())?;
            let archive = fs::read(compressed_archive_path)?;

            let restored_files = match HttpCache::restore_tar(&anchor, &archive) {
                Ok(restored_files) => restored_files,
                Err(err) => {
                    if let Some(expected_error) = test.expected_error {
                        #[cfg(unix)]
                        assert_eq!(err.to_string(), expected_error.unix);
                        #[cfg(windows)]
                        assert_eq!(err.to_string(), expected_error.windows);
                        continue;
                    } else {
                        return Err(err.into());
                    }
                }
            };

            let expected_files = test.expected_files;
            assert_eq!(restored_files.len(), test.input_files.len());
            for expected_file in expected_files {
                assert_file_exists(&anchor, &expected_file)?;
            }
        }

        Ok(())
    }

    #[derive(Debug)]
    struct TarTestCase {
        expected_entries: HashSet<&'static str>,
        tar_file: &'static [u8],
    }

    fn get_tar_test_cases() -> Vec<TarTestCase> {
        vec![
            TarTestCase {
                expected_entries: vec![
                    "apps/web/.next/",
                    "apps/web/.next/BUILD_ID",
                    "apps/web/.next/build-manifest.json",
                    "apps/web/.next/cache/",
                    "apps/web/.next/export-marker.json",
                    "apps/web/.next/images-manifest.json",
                    "apps/web/.next/next-server.js.nft.json",
                    "apps/web/.next/package.json",
                    "apps/web/.next/prerender-manifest.json",
                    "apps/web/.next/react-loadable-manifest.json",
                    "apps/web/.next/required-server-files.json",
                    "apps/web/.next/routes-manifest.json",
                    "apps/web/.next/server/",
                    "apps/web/.next/server/chunks/",
                    "apps/web/.next/server/chunks/font-manifest.json",
                    "apps/web/.next/server/font-manifest.json",
                    "apps/web/.next/server/middleware-build-manifest.js",
                    "apps/web/.next/server/middleware-manifest.json",
                    "apps/web/.next/server/middleware-react-loadable-manifest.js",
                    "apps/web/.next/server/next-font-manifest.js",
                    "apps/web/.next/server/next-font-manifest.json",
                    "apps/web/.next/server/pages/",
                    "apps/web/.next/server/pages-manifest.json",
                    "apps/web/.next/server/pages/404.html",
                    "apps/web/.next/server/pages/500.html",
                    "apps/web/.next/server/pages/_app.js",
                    "apps/web/.next/server/pages/_app.js.nft.json",
                    "apps/web/.next/server/pages/_document.js",
                    "apps/web/.next/server/pages/_document.js.nft.json",
                    "apps/web/.next/server/pages/_error.js",
                    "apps/web/.next/server/pages/_error.js.nft.json",
                    "apps/web/.next/server/pages/index.html",
                    "apps/web/.next/server/pages/index.js.nft.json",
                    "apps/web/.next/server/webpack-runtime.js",
                    "apps/web/.next/static/",
                    "apps/web/.next/static/cYEK0_ogl5lSUpJIM1e5A/",
                    "apps/web/.next/static/cYEK0_ogl5lSUpJIM1e5A/_buildManifest.js",
                    "apps/web/.next/static/cYEK0_ogl5lSUpJIM1e5A/_ssgManifest.js",
                    "apps/web/.next/static/chunks/",
                    "apps/web/.next/static/chunks/framework-ffffd4e8198d9762.js",
                    "apps/web/.next/static/chunks/main-7595d4b6af4c2f6f.js",
                    "apps/web/.next/static/chunks/pages/",
                    "apps/web/.next/static/chunks/pages/_app-09b371265aa7309d.js",
                    "apps/web/.next/static/chunks/pages/_error-e0615e853f5988ee.js",
                    "apps/web/.next/static/chunks/pages/index-c7599f437e77dab6.js",
                    "apps/web/.next/static/chunks/polyfills-c67a75d1b6f99dc8.js",
                    "apps/web/.next/static/chunks/webpack-4e7214a60fad8e88.js",
                    "apps/web/.next/trace",
                    "apps/web/.turbo/turbo-build.log",
                ]
                .into_iter()
                .collect(),
                tar_file: include_bytes!("../examples/627737318531b1db.tar.zst"),
            },
            TarTestCase {
                expected_entries: vec![
                    "apps/docs/.next/",
                    "apps/docs/.next/BUILD_ID",
                    "apps/docs/.next/build-manifest.json",
                    "apps/docs/.next/cache/",
                    "apps/docs/.next/export-marker.json",
                    "apps/docs/.next/images-manifest.json",
                    "apps/docs/.next/next-server.js.nft.json",
                    "apps/docs/.next/package.json",
                    "apps/docs/.next/prerender-manifest.json",
                    "apps/docs/.next/react-loadable-manifest.json",
                    "apps/docs/.next/required-server-files.json",
                    "apps/docs/.next/routes-manifest.json",
                    "apps/docs/.next/server/",
                    "apps/docs/.next/server/chunks/",
                    "apps/docs/.next/server/chunks/font-manifest.json",
                    "apps/docs/.next/server/font-manifest.json",
                    "apps/docs/.next/server/middleware-build-manifest.js",
                    "apps/docs/.next/server/middleware-manifest.json",
                    "apps/docs/.next/server/middleware-react-loadable-manifest.js",
                    "apps/docs/.next/server/next-font-manifest.js",
                    "apps/docs/.next/server/next-font-manifest.json",
                    "apps/docs/.next/server/pages/",
                    "apps/docs/.next/server/pages-manifest.json",
                    "apps/docs/.next/server/pages/404.html",
                    "apps/docs/.next/server/pages/500.html",
                    "apps/docs/.next/server/pages/_app.js",
                    "apps/docs/.next/server/pages/_app.js.nft.json",
                    "apps/docs/.next/server/pages/_document.js",
                    "apps/docs/.next/server/pages/_document.js.nft.json",
                    "apps/docs/.next/server/pages/_error.js",
                    "apps/docs/.next/server/pages/_error.js.nft.json",
                    "apps/docs/.next/server/pages/index.html",
                    "apps/docs/.next/server/pages/index.js.nft.json",
                    "apps/docs/.next/server/webpack-runtime.js",
                    "apps/docs/.next/static/",
                    "apps/docs/.next/static/3WdlFLztLa3kpcDUT_2Yc/",
                    "apps/docs/.next/static/3WdlFLztLa3kpcDUT_2Yc/_buildManifest.js",
                    "apps/docs/.next/static/3WdlFLztLa3kpcDUT_2Yc/_ssgManifest.js",
                    "apps/docs/.next/static/chunks/",
                    "apps/docs/.next/static/chunks/framework-ffffd4e8198d9762.js",
                    "apps/docs/.next/static/chunks/main-7595d4b6af4c2f6f.js",
                    "apps/docs/.next/static/chunks/pages/",
                    "apps/docs/.next/static/chunks/pages/_app-09b371265aa7309d.js",
                    "apps/docs/.next/static/chunks/pages/_error-e0615e853f5988ee.js",
                    "apps/docs/.next/static/chunks/pages/index-53e8899c12a3a0e1.js",
                    "apps/docs/.next/static/chunks/polyfills-c67a75d1b6f99dc8.js",
                    "apps/docs/.next/static/chunks/webpack-4e7214a60fad8e88.js",
                    "apps/docs/.next/trace",
                    "apps/docs/.turbo/turbo-build.log",
                ]
                .into_iter()
                .collect(),
                tar_file: include_bytes!("../examples/c5da7df87dc01a95.tar.zst"),
            },
        ]
    }

    #[test]
    fn test_restore_tar() -> Result<()> {
        let test_cases = get_tar_test_cases();

        for test_case in test_cases {
            let temp_dir = tempdir()?;
            let files = HttpCache::restore_tar(
                &AbsoluteSystemPathBuf::new(temp_dir.path())?,
                test_case.tar_file,
            )?;

            assert_eq!(files.len(), test_case.expected_entries.len());

            for file in files {
                assert!(test_case
                    .expected_entries
                    .contains(file.as_path().to_str().unwrap()));
            }
        }

        Ok(())
    }
}
