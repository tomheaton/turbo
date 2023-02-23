use std::{backtrace::Backtrace, env::current_dir, fs, os};

use log::info;
use tar::{Archive, EntryType, Header};
use turbopath::{
    AbsoluteSystemPathBuf, AnchoredSystemPathBuf, RelativeSystemPathBuf, RelativeUnixPathBuf,
};
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
                .ok_or(CacheError::ArtifactTagMissing)?;

            let expected_tag = expected_tag
                .to_str()
                .map_err(|_| CacheError::InvalidTag(Backtrace::capture()))?
                .to_string();

            let body = response.bytes().await.map_err(|e| {
                CacheError::ApiClientError(turborepo_api_client::Error::ReqwestError(e))
            })?;
            let is_valid = signer_verifier.validate(hash, &body, &expected_tag)?;

            if !is_valid {
                return Err(CacheError::InvalidTag(Backtrace::capture()));
            }

            body
        } else {
            response.bytes().await.map_err(|e| {
                CacheError::ApiClientError(turborepo_api_client::Error::ReqwestError(e))
            })?
        };

        let files = Self::restore_tar(&self.repo_root, &body)?;

        Ok((files, duration))
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
                ));
            }
            let header = entry.header();
            match header.entry_type() {
                EntryType::Directory => {
                    info!("Restoring directory {}", filename.to_string_lossy());
                    fs::create_dir_all(&filename)?;
                }
                EntryType::Regular => {
                    info!("Restoring file {}", filename.to_string_lossy());

                    if let Some(parent) = filename.parent() {
                        if parent.as_ref() != current_dir()?.as_path() {
                            fs::create_dir_all(parent)?;
                        }
                    }

                    entry.unpack(&filename)?;
                }
                EntryType::Symlink => {
                    info!("Restoring symlink {}", filename.to_string_lossy());

                    if let Err(CacheError::LinkTargetDoesNotExist(_)) =
                        Self::restore_symlink(root, header, false)
                    {
                        missing_links.push(header.clone());
                    }
                }
                entry_type => {
                    println!(
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
        let link_name = header
            .link_name()?
            .ok_or_else(|| CacheError::LinkNameDoesNotExist)?;

        let relative_link_target = RelativeSystemPathBuf::from_path(link_name)?;
        let header_path: AnchoredSystemPathBuf = header.path()?.as_ref().try_into()?;
        let link_filename = root.resolve(&header_path);
        fs::create_dir_all(link_filename.parent().ok_or_else(|| {
            CacheError::InvalidFilePath(link_filename.to_string_lossy().to_string())
        })?)?;

        let link_target = link_filename
            .parent()
            .ok_or_else(|| {
                CacheError::InvalidFilePath(link_filename.to_string_lossy().to_string())
            })?
            .join_relative(&relative_link_target);

        if !link_target.exists() && !allow_nonexistent_targets {
            return Err(CacheError::LinkTargetDoesNotExist(
                link_filename.to_string_lossy().to_string(),
            ));
        }
        if link_filename.exists() {
            fs::remove_file(&link_filename)?;
        }

        #[cfg(unix)]
        os::unix::fs::symlink(link_target, link_filename)?;
        #[cfg(windows)]
        os::windows::fs::symlink_file(link_target, link_filename)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use anyhow::Result;
    use turbopath::AbsoluteSystemPathBuf;

    use crate::http::HttpCache;

    struct TestCase {
        expected_entries: HashSet<&'static str>,
        tar_file: &'static [u8],
    }

    fn get_test_cases() -> Vec<TestCase> {
        vec![
            TestCase {
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
            TestCase {
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
        env_logger::init();
        let test_cases = get_test_cases();

        for test_case in test_cases {
            let temp_dir = tempfile::tempdir()?;
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
