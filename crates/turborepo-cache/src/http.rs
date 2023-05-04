use std::{backtrace::Backtrace, env::current_dir, fs, os, path::Path};

use tar::{Archive, EntryType, Header};
use tracing::{debug, error, info};
use turbopath::{AbsoluteSystemPathBuf, AnchoredSystemPathBuf, RelativeSystemPathBuf};
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
            let restored_name = RelativeSystemPathBuf::new(entry.path()?)?;
            let restored_anchored_path = restored_name.into();
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
                        if parent.as_path() != current_dir()?.as_path() {
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
