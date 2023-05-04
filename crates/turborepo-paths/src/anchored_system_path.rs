use std::{fmt, path::Path};

use crate::AnchoredSystemPathBuf;
pub struct AnchoredSystemPath(Path);

impl ToOwned for AnchoredSystemPath {
    type Owned = AnchoredSystemPathBuf;

    fn to_owned(&self) -> Self::Owned {
        AnchoredSystemPathBuf(self.0.to_owned())
    }
}

impl AsRef<AnchoredSystemPath> for AnchoredSystemPath {
    fn as_ref(&self) -> &AnchoredSystemPath {
        self
    }
}

impl fmt::Display for AnchoredSystemPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.display().fmt(f)
    }
}

impl AsRef<Path> for AnchoredSystemPath {
    fn as_ref(&self) -> &Path {
        &self.0
    }
}

impl AnchoredSystemPath {
    pub(crate) fn new_unchecked(path: &Path) -> &Self {
        unsafe { &*(path as *const Path as *const Self) }
    }

    pub fn parent(&self) -> Option<&AnchoredSystemPath> {
        self.0.parent().map(AnchoredSystemPath::new_unchecked)
    }

    pub fn as_path(&self) -> &Path {
        &self.0
    }
}
