use bstr::{BString, ByteSlice};

use crate::{PathError, RelativeUnixPathBuf};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct AnchoredUnixPathBuf(BString);

impl AnchoredUnixPathBuf {
    pub fn into_inner(self) -> BString {
        self.0
    }

    pub fn make_canonical_for_tar(&mut self, is_dir: bool) {
        if is_dir {
            if !self.0.ends_with(b"/") {
                self.0.push(b'/');
            }
        }
    }

    pub fn as_str(&self) -> Result<&str, PathError> {
        self.0.to_str().or_else(|_| {
            Err(PathError::InvalidUnicode(
                self.0.as_bytes().to_str_lossy().to_string(),
            ))
        })
    }
}

impl Into<RelativeUnixPathBuf> for AnchoredUnixPathBuf {
    fn into(self) -> RelativeUnixPathBuf {
        unsafe { RelativeUnixPathBuf::unchecked_new(self.0) }
    }
}

impl From<RelativeUnixPathBuf> for AnchoredUnixPathBuf {
    fn from(path: RelativeUnixPathBuf) -> Self {
        AnchoredUnixPathBuf(path.into_inner())
    }
}
