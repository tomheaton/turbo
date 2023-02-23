use turbopath::AbsoluteSystemPathBuf;
use turborepo_api_client::APIClient;
use turborepo_cache::{http::HttpCache, signature_authentication::ArtifactSignatureAuthenticator};

use crate::{proto, Buffer};

#[no_mangle]
#[tokio::main]
pub async extern "C" fn retrieve(buffer: Buffer) -> Buffer {
    let req: proto::RetrieveRequest = match buffer.into_proto() {
        Ok(req) => req,
        Err(err) => {
            let resp = proto::RetrieveResponse {
                response: Some(proto::retrieve_response::Response::Error(err.to_string())),
            };
            return resp.into();
        }
    };

    let http_cache_config = req.http_cache.expect("http_cache is required field");

    let api_client_config = http_cache_config
        .api_client
        .expect("api_client is required field");

    let api_client = APIClient::new(
        &api_client_config.base_url,
        api_client_config.timeout,
        &api_client_config.version,
    )
    .expect("API client failed to build");

    let artifact_signature_authenticator = http_cache_config
        .authenticator
        .map(|config| ArtifactSignatureAuthenticator::new(config.team_id));

    let Ok(repo_root) = AbsoluteSystemPathBuf::new(http_cache_config.repo_root) else {
        let resp = proto::RetrieveResponse {
            response: Some(proto::retrieve_response::Response::Error(
                "repo_root is not absolute path".to_string(),
            )),
        };
        return resp.into();
    };

    let http_cache = HttpCache::new(api_client, artifact_signature_authenticator, repo_root);

    match http_cache
        .retrieve(
            &req.hash,
            &api_client_config.token,
            &api_client_config.team_id,
            api_client_config.team_slug.as_deref(),
            api_client_config.use_preflight,
        )
        .await
    {
        Ok((file_paths, duration)) => {
            let mut files = Vec::new();
            for path in file_paths {
                let path_str = match path.to_str() {
                    Ok(path_str) => path_str,
                    Err(err) => {
                        let resp = proto::RetrieveResponse {
                            response: Some(proto::retrieve_response::Response::Error(
                                err.to_string(),
                            )),
                        };
                        return resp.into();
                    }
                };

                files.push(path_str.to_string())
            }

            proto::RetrieveResponse {
                response: Some(proto::retrieve_response::Response::Files(
                    proto::RestoredFilesList { files, duration },
                )),
            }
            .into()
        }
        Err(err) => proto::RetrieveResponse {
            response: Some(proto::retrieve_response::Response::Error(err.to_string())),
        }
        .into(),
    }
}
