use std::{backtrace::Backtrace, fs, fs::OpenOptions, io::Write};

use tar::{EntryType, Header};
use turbopath::{AbsoluteSystemPath, AnchoredSystemPath, AnchoredUnixPathBuf, RelativeUnixPathBuf};

use crate::CacheError;

pub struct CacheWriter<'a> {
    tar_builder: tar::Builder<Box<dyn Write + 'a>>,
}

// Lets windows know that we're going to be reading this file sequentially
#[cfg(windows)]
pub const FILE_FLAG_SEQUENTIAL_SCAN: u32 = 0x08000000;

impl<'a> CacheWriter<'a> {
    // Creates a CacheArchive using the specified writer. Always compresses the
    // archive.
    pub fn create_with_writer(writer: impl Write + 'a) -> Result<Self, CacheError> {
        let zw = zstd::Encoder::new(writer, 0)?;
        let tar_builder: tar::Builder<Box<dyn Write>> = tar::Builder::new(Box::new(zw));

        Ok(Self { tar_builder })
    }

    pub fn add_file(
        &mut self,
        anchor: &AbsoluteSystemPath,
        file_path: &AnchoredSystemPath,
    ) -> Result<(), CacheError> {
        let source_path = anchor.resolve(file_path);

        let file_info = fs::symlink_metadata(source_path.as_path())?;
        let cache_destination_name = RelativeUnixPathBuf::new(file_path.to_str()?.as_bytes())?;

        let mut header = Self::create_header(cache_destination_name.into(), &file_info)?;
        if file_info.is_symlink() {
            let link = fs::read_link(source_path.as_path())?;
            header.set_link_name(link)?;
        }

        // Throw an error if trying to create a cache that contains a type we don't
        // support.
        if !matches!(
            header.entry_type(),
            EntryType::Regular | EntryType::Directory | EntryType::Symlink
        ) {
            return Err(CacheError::UnsupportedFileType(
                header.entry_type(),
                Backtrace::capture(),
            ));
        }

        // Consistent creation
        header.set_uid(0);
        header.set_gid(0);
        header.as_gnu_mut().unwrap().set_atime(0);
        header.set_mtime(0);
        header.as_gnu_mut().unwrap().set_ctime(0);

        if matches!(header.entry_type(), EntryType::Regular) && header.size()? > 0 {
            let file = OpenOptions::new().read(true).open(source_path.as_path())?;
            self.tar_builder.append(&header, file)?;
        } else {
            self.tar_builder.append(&header, &mut std::io::empty())?;
        }

        Ok(())
    }

    fn create_header(
        mut path: AnchoredUnixPathBuf,
        file_info: &fs::Metadata,
    ) -> Result<Header, CacheError> {
        let mut header = Header::new_gnu();

        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            header.set_mode(file_info.mode());
        }
        path.make_canonical_for_tar(file_info.is_dir());
        header.set_path(path.as_str()?)?;

        Ok(header)
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    #[test]
    fn test_add_trailing_slash() {
        let mut path = PathBuf::from("foo/bar");
        assert_eq!(path.to_string_lossy(), "foo/bar");
        path.push("");
        assert_eq!(path.to_string_lossy(), "foo/bar/");

        // Confirm that this is idempotent
        path.push("");
        assert_eq!(path.to_string_lossy(), "foo/bar/");
    }
}
