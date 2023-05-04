use std::{
    backtrace::Backtrace,
    fs,
    fs::{File, OpenOptions},
    io::{BufWriter, Read},
};

use tar::{EntryType, Header};
use turbopath::{
    AbsoluteSystemPathBuf, AnchoredSystemPathBuf, AnchoredUnixPathBuf, RelativeUnixPathBuf,
};

use crate::CacheError;

enum CacheWriter<'a> {
    Compressed(tar::Builder<zstd::Encoder<'a, BufWriter<File>>>),
    Uncompressed(tar::Builder<BufWriter<File>>),
}

impl<'a> CacheWriter<'a> {
    fn append(&mut self, header: &Header, body: impl Read) -> Result<(), CacheError> {
        match self {
            CacheWriter::Compressed(builder) => Ok(builder.append(header, body)?),
            CacheWriter::Uncompressed(builder) => Ok(builder.append(header, body)?),
        }
    }
}

struct CacheArchive<'a> {
    // The location on disk for the archive
    path: AbsoluteSystemPathBuf,
    writer: CacheWriter<'a>,
}

// Lets windows know that we're going to be reading this file sequentially
#[cfg(windows)]
pub const FILE_FLAG_SEQUENTIAL_SCAN: u32 = 0x08000000;

impl<'a> CacheArchive<'a> {
    fn create(path: AbsoluteSystemPathBuf) -> Result<Self, CacheError> {
        let mut options = OpenOptions::new();

        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt;

            options.custom_flags(FILE_FLAG_SEQUENTIAL_SCAN);
        }

        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;

            options.mode(0o644);
        }

        let file = options
            .write(true)
            .create(true)
            .truncate(true)
            .append(true)
            .open(path.as_path())?;

        let file_buffer = BufWriter::with_capacity(2usize.pow(20), file);

        let is_compressed = path.as_path().ends_with(".zst");
        let writer = if is_compressed {
            let zw = zstd::Encoder::new(file_buffer, 0)?;

            CacheWriter::Compressed(tar::Builder::new(zw))
        } else {
            CacheWriter::Uncompressed(tar::Builder::new(file_buffer))
        };

        Ok(Self { path, writer })
    }

    fn add_file(
        &mut self,
        anchor: &AbsoluteSystemPathBuf,
        file_path: &AnchoredSystemPathBuf,
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
            self.writer.append(&header, file)?;
        } else {
            self.writer.append(&header, &mut std::io::empty())?;
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
