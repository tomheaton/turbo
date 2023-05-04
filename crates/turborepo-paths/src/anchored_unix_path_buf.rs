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
            // This is a hacky way to add a trailing slash
            // to a path in Rust. This works because Rust defines
            // push in terms of path components, so you can think of this
            // as adding an empty component to the end.
            // The alternative is two separate implementations for Windows and
            // Unix that allocate.
            if !self.0.ends_with(b"/") {
                self.0.push(b'/');
            }
        }
    }

    pub fn as_str(&self) -> Result<&str, PathError> {
        let s = self
            .0
            .to_str()
            .or_else(|_| Err(PathError::Utf8Error(self.0.as_bytes().to_owned())))?;
        Ok(s)
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
