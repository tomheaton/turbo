use std::env;

use base64::{prelude::BASE64_STANDARD, Engine};
use os_str_bytes::OsStringBytes;
use ring::{
    hmac,
    hmac::{Tag, HMAC_SHA256},
};
use serde::Serialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SignatureError {
    #[error(
        "signature secret key not found. You must specify a secret key in the \
         TURBO_REMOTE_CACHE_SIGNATURE_KEY environment variable"
    )]
    NoSignatureSecretKey,
    #[error("serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),
    #[error("base64 encoding error: {0}")]
    Base64EncodingError(#[from] base64::DecodeError),
}

#[derive(Debug)]
pub struct ArtifactSignatureAuthenticator {
    team_id: String,
}

#[derive(Debug, Serialize)]
struct ArtifactSignature {
    hash: String,
    #[serde(rename = "teamId")]
    team_id: String,
}

impl ArtifactSignatureAuthenticator {
    pub fn new(team_id: String) -> Self {
        Self { team_id }
    }

    fn secret_key(&self) -> Result<Vec<u8>, SignatureError> {
        Ok(env::var_os("TURBO_REMOTE_CACHE_SIGNATURE_KEY")
            .ok_or(SignatureError::NoSignatureSecretKey)?
            .into_raw_vec())
    }

    fn construct_metadata(&self, hash: &str) -> Result<String, SignatureError> {
        let metadata = serde_json::to_string(&ArtifactSignature {
            hash: hash.to_string(),
            team_id: self.team_id.clone(),
        })?;

        Ok(metadata)
    }

    fn get_tag_generator(&self, hash: &str) -> Result<hmac::Context, SignatureError> {
        let secret_key = hmac::Key::new(HMAC_SHA256, &self.secret_key()?);
        let metadata = self.construct_metadata(hash)?;

        let mut hmac_ctx = hmac::Context::with_key(&secret_key);
        hmac_ctx.update(metadata.as_bytes());

        Ok(hmac_ctx)
    }

    pub fn generate_tag_bytes(
        &self,
        hash: &str,
        artifact_body: &[u8],
    ) -> Result<Tag, SignatureError> {
        let mut hmac_ctx = self.get_tag_generator(hash)?;

        hmac_ctx.update(artifact_body);
        let hmac_output = hmac_ctx.sign();
        Ok(hmac_output)
    }

    pub fn generate_tag(&self, hash: &str, artifact_body: &[u8]) -> Result<String, SignatureError> {
        let mut hmac_ctx = self.get_tag_generator(hash)?;

        hmac_ctx.update(artifact_body);
        let hmac_output = hmac_ctx.sign();
        Ok(BASE64_STANDARD.encode(hmac_output))
    }

    pub fn validate_tag(
        &self,
        hash: &str,
        artifact_body: &[u8],
        expected_tag: &[u8],
    ) -> Result<bool, SignatureError> {
        let secret_key = hmac::Key::new(HMAC_SHA256, &self.secret_key()?);
        let mut message = self.construct_metadata(hash)?.into_bytes();
        message.extend(artifact_body);
        Ok(hmac::verify(&secret_key, &message, expected_tag).is_ok())
    }

    pub fn validate(
        &self,
        hash: &str,
        artifact_body: &bytes::Bytes,
        expected_tag: &str,
    ) -> Result<bool, SignatureError> {
        let secret_key = hmac::Key::new(HMAC_SHA256, &self.secret_key()?);
        let mut message = self.construct_metadata(hash)?.into_bytes();
        message.extend(artifact_body);
        let expected_bytes = BASE64_STANDARD.decode(expected_tag)?;
        Ok(hmac::verify(&secret_key, &message, &expected_bytes).is_ok())
    }
}

// #[cfg(test)]
// mod tests {
//     use std::{ffi::OsStr, str::from_utf8};
//
//     use anyhow::Result;
//     use os_str_bytes::OsStrBytes;
//
//     use super::*;
//
//     #[test]
//     fn test_signatures() -> Result<()> {
//         for test_case in get_test_cases() {
//             test_signature(test_case)?;
//         }
//         Ok(())
//     }
//
//     fn test_signature(test_case: TestCase) -> Result<()> {
//         env::set_var("TURBO_REMOTE_CACHE_SIGNATURE_KEY",
// test_case.secret_key);         let signature =
// ArtifactSignatureAuthentication {             team_id:
// test_case.team_id.to_string(),         };
//
//         let hash = test_case.artifact_hash;
//         let artifact_body = &test_case.artifact_body;
//         let tag = signature.generate_tag_bytes(hash, artifact_body)?;
//
//         assert!(signature.validate_tag(hash, artifact_body, tag.as_ref())?);
//         if !signature.validate_tag(hash, artifact_body, &test_case.hmac_tag)?
// {             println!("---------------------");
//             println!("test case: {:?}", test_case);
//             println!("expected tag: {:?}", test_case.hmac_tag);
//             println!("received tag: {:?}", tag.as_ref());
//             println!("expected metadata: {}", test_case.metadata);
//             println!("received metadata: {}",
// signature.construct_metadata(hash)?);
// println!("---------------------");             panic!();
//         } else {
//             println!("test case: {:?}", test_case);
//         }
//
//         // Generate some bad tag that is not correct
//         let bad_tag = BASE64_STANDARD.encode(b"bad tag");
//         assert!(!signature.validate(hash, artifact_body, &bad_tag)?);
//
//         // Change the key (to something that is not a valid unicode string)
//         env::set_var(
//             "TURBO_REMOTE_CACHE_SIGNATURE_KEY",
//             OsStr::assert_from_raw_bytes([0xf0, 0x28, 0x8c, 0xbc].as_ref()),
//         );
//
//         // Confirm that the tag is no longer valid
//         assert!(!signature.validate_tag(hash, artifact_body, tag.as_ref())?);
//
//         // Generate new tag
//         let tag = signature.generate_tag(hash, artifact_body)?;
//
//         // Confirm it's valid
//         assert!(signature.validate(hash, artifact_body, &tag)?);
//         Ok(())
//     }
// }
