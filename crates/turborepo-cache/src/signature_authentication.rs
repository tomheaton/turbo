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
        artifact_body: &[u8],
        expected_tag: &str,
    ) -> Result<bool, SignatureError> {
        let secret_key = hmac::Key::new(HMAC_SHA256, &self.secret_key()?);
        let mut message = self.construct_metadata(hash)?.into_bytes();
        message.extend(artifact_body);
        let expected_bytes = BASE64_STANDARD.decode(expected_tag)?;
        Ok(hmac::verify(&secret_key, &message, &expected_bytes).is_ok())
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsStr;

    use anyhow::Result;
    use os_str_bytes::OsStrBytes;

    use super::*;

    struct TestCase {
        secret_key: &'static str,
        team_id: &'static str,
        artifact_hash: &'static str,
        artifact_body: &'static [u8],
    }

    fn get_test_cases() -> Vec<TestCase> {
        vec![
            TestCase {
                secret_key: "x3vq8mFz0J",
                team_id: "tH7sL1Rn9K",
                artifact_hash: "d5b7e4688f",
                artifact_body: &[5, 72, 219, 39, 156],
            },
            TestCase {
                secret_key: "r8cP5sTn0Y",
                team_id: "sL2vM9Qj1D",
                artifact_hash: "a1c8f3e3d7",
                artifact_body: &[128, 234, 49, 67, 96],
            },
            TestCase {
                secret_key: "g4kS2nDv6L",
                team_id: "mB8pF9hJ0X",
                artifact_hash: "f2e6d4a2c1",
                artifact_body: &[217, 88, 71, 16, 53],
            },
            TestCase {
                secret_key: "j0fT3qPz6N",
                team_id: "cH1rK7vD5B",
                artifact_hash: "e8a5c7f0b2",
                artifact_body: &[202, 12, 104, 90, 182],
            },
            TestCase {
                secret_key: "w1xM5bVz2Q",
                team_id: "sL9cJ0nK7F",
                artifact_hash: "c4e6f9a1d8",
                artifact_body: &[67, 93, 241, 78, 192],
            },
            TestCase {
                secret_key: "f9gD2tNc8K",
                team_id: "pJ1xL6rF0V",
                artifact_hash: "b3a9c5e8f7",
                artifact_body: &[23, 160, 36, 208, 97],
            },
            TestCase {
                secret_key: "k5nB1tLc9Z",
                team_id: "wF0xV8jP7G",
                artifact_hash: "e7a9c1b8f6",
                artifact_body: &[237, 148, 107, 51, 241],
            },
            TestCase {
                secret_key: "d8mR2vZn5X",
                team_id: "kP6cV1jN7T",
                artifact_hash: "f2c8e7b6a1",
                artifact_body: &[128, 36, 180, 67, 230],
            },
            TestCase {
                secret_key: "p4kS5nHv3L",
                team_id: "tR1cF2bD0M",
                artifact_hash: "d5b8e4f3c9",
                artifact_body: &[47, 161, 218, 119, 223],
            },
            TestCase {
                secret_key: "j5nG1bDv6X",
                team_id: "tH8rK0pJ3L",
                artifact_hash: "e3c5a9b2f1",
                artifact_body: &[188, 245, 109, 12, 167],
            },
            TestCase {
                secret_key: "f2cB1tLm9X",
                team_id: "rG7sK0vD4N",
                artifact_hash: "b5a9c8e3f6",
                artifact_body: &[205, 154, 83, 60, 27],
            },
            TestCase {
                secret_key: "t1sN2mFj8Z",
                team_id: "pK3cH7rD6B",
                artifact_hash: "d4e9c1f7b6",
                artifact_body: &[226, 245, 85, 79, 136],
            },
            TestCase {
                secret_key: "h5jM3pZv8X",
                team_id: "dR1bF2cK6L",
                artifact_hash: "f2e6d5b1c8",
                artifact_body: &[70, 184, 71, 150, 238],
            },
            TestCase {
                secret_key: "n0cT2bDk9J",
                team_id: "pJ3sF6rM8N",
                artifact_hash: "e4a9d7c1f8",
                artifact_body: &[240, 130, 13, 167, 75],
            },
            TestCase {
                secret_key: "b2dV6kPf9X",
                team_id: "tN3cH7mK8J",
                artifact_hash: "c9e3d7b6f8",
                artifact_body: &[58, 42, 80, 138, 189],
            },
        ]
    }

    #[test]
    fn test_signatures() -> Result<()> {
        for test_case in get_test_cases() {
            test_signature(test_case)?;
        }
        Ok(())
    }

    fn test_signature(test_case: TestCase) -> Result<()> {
        env::set_var("TURBO_REMOTE_CACHE_SIGNATURE_KEY", test_case.secret_key);
        let signature = ArtifactSignatureAuthenticator {
            team_id: test_case.team_id.to_string(),
        };

        let hash = test_case.artifact_hash;
        let artifact_body = &test_case.artifact_body;
        let tag = signature.generate_tag_bytes(hash, artifact_body)?;

        assert!(signature.validate_tag(hash, artifact_body, tag.as_ref())?);

        // Generate some bad tag that is not correct
        let bad_tag = BASE64_STANDARD.encode(b"bad tag");
        assert!(!signature.validate(hash, artifact_body, &bad_tag)?);

        // Change the key (to something that is not a valid unicode string)
        env::set_var(
            "TURBO_REMOTE_CACHE_SIGNATURE_KEY",
            OsStr::assert_from_raw_bytes([0xf0, 0x28, 0x8c, 0xbc].as_ref()),
        );

        // Confirm that the tag is no longer valid
        assert!(!signature.validate_tag(hash, artifact_body, tag.as_ref())?);

        // Generate new tag
        let tag = signature.generate_tag(hash, artifact_body)?;

        // Confirm it's valid
        assert!(signature.validate(hash, artifact_body, &tag)?);
        Ok(())
    }
}
