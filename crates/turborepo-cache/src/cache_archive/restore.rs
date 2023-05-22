use std::{
    backtrace::Backtrace,
    collections::HashMap,
    ffi::OsStr,
    fs,
    fs::OpenOptions,
    io::Read,
    path::{Path, PathBuf},
};

use petgraph::graph::DiGraph;
use tar::Entry;
use turbopath::{AbsoluteSystemPath, AbsoluteSystemPathBuf, AnchoredSystemPathBuf};

use crate::{
    cache_archive::{
        restore_directory::restore_directory,
        restore_regular::restore_regular,
        restore_symlink::{
            canonicalize_linkname, restore_symlink, restore_symlink_with_missing_target,
        },
    },
    CacheError,
};

pub struct CacheReader<'a> {
    reader: Box<dyn Read + 'a>,
}

impl<'a> CacheReader<'a> {
    pub fn from_reader(reader: impl Read + 'a, is_compressed: bool) -> Result<Self, CacheError> {
        let reader: Box<dyn Read> = if is_compressed {
            Box::new(zstd::Decoder::new(reader)?)
        } else {
            Box::new(reader)
        };

        Ok(CacheReader { reader })
    }

    pub fn open(path: &AbsoluteSystemPathBuf) -> Result<Self, CacheError> {
        let mut options = OpenOptions::new();

        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;

            options.mode(0o777);
        }

        #[cfg(windows)]
        {
            use crate::cache_archive::create::FILE_FLAG_SEQUENTIAL_SCAN;
            options.custom_flags(FILE_FLAG_SEQUENTIAL_SCAN);
        }

        let file = options.read(true).open(path.as_path())?;

        let reader: Box<dyn Read> = if path.as_path().extension() == Some(OsStr::new("zst")) {
            Box::new(zstd::Decoder::new(file)?)
        } else {
            Box::new(file)
        };

        Ok(CacheReader { reader })
    }

    pub fn restore(
        &mut self,
        anchor: &AbsoluteSystemPath,
    ) -> Result<Vec<AnchoredSystemPathBuf>, CacheError> {
        let mut restored = Vec::new();
        fs::create_dir_all(anchor.as_path())?;

        // We're going to make the following two assumptions here for "fast"
        // path restoration:
        // - All directories are enumerated in the `tar`.
        // - The contents of the tar are enumerated depth-first.
        //
        // This allows us to avoid:
        // - Attempts at recursive creation of directories.
        // - Repetitive `lstat` on restore of a file.
        //
        // Violating these assumptions won't cause things to break but we're
        // only going to maintain an `lstat` cache for the current tree.
        // If you violate these assumptions and the current cache does
        // not apply for your path, it will clobber and re-start from the common
        // shared prefix.
        let mut tr = tar::Archive::new(&mut self.reader);

        Self::restore_entries(&mut tr, &mut restored, anchor)?;
        Ok(restored)
    }
    fn restore_entries<'b, T: Read>(
        tr: &'b mut tar::Archive<T>,
        restored: &mut Vec<AnchoredSystemPathBuf>,
        anchor: &AbsoluteSystemPath,
    ) -> Result<(), CacheError> {
        // On first attempt to restore it's possible that a link target doesn't exist.
        // Save them and topologically sort them.
        let mut symlinks = Vec::new();

        for entry in tr.entries()? {
            let mut entry = entry?;
            match restore_entry(anchor, &mut entry) {
                Err(CacheError::LinkTargetDoesNotExist(_, _)) => {
                    symlinks.push(entry);
                }
                Err(e) => return Err(e),
                Ok(restored_path) => restored.push(restored_path),
            }
        }

        let mut restored_symlinks = Self::topologically_restore_symlinks(anchor, &symlinks)?;
        restored.append(&mut restored_symlinks);
        Ok(())
    }

    fn topologically_restore_symlinks<'c, T: Read>(
        anchor: &AbsoluteSystemPath,
        symlinks: &[Entry<'c, T>],
    ) -> Result<Vec<AnchoredSystemPathBuf>, CacheError> {
        let mut graph = DiGraph::new();
        let mut header_lookup = HashMap::new();
        let mut restored = Vec::new();
        let mut nodes = HashMap::new();

        for entry in symlinks {
            let processed_name = canonicalize_name(&entry.header().path()?)?;
            let processed_sourcename =
                canonicalize_linkname(anchor, &processed_name, processed_name.as_path())?;
            // symlink must have a linkname
            let linkname = entry
                .header()
                .link_name()?
                .expect("symlink without linkname");

            let processed_linkname = canonicalize_linkname(anchor, &processed_name, &linkname)?;

            let source_node = *nodes
                .entry(processed_sourcename.clone())
                .or_insert_with(|| graph.add_node(processed_sourcename.clone()));
            let link_node = *nodes
                .entry(processed_linkname.clone())
                .or_insert_with(|| graph.add_node(processed_linkname.clone()));

            graph.add_edge(source_node, link_node, ());

            header_lookup.insert(processed_sourcename, entry.header().clone());
        }

        let nodes = petgraph::algo::toposort(&graph, None)
            .map_err(|_| CacheError::CycleDetected(Backtrace::capture()))?;

        for node in nodes {
            let key = &graph[node];

            let Some(header) = header_lookup.get(key) else {
                continue
            };
            let file = restore_symlink_with_missing_target(anchor, header)?;
            restored.push(file);
        }

        Ok(restored)
    }
}

fn restore_entry<T: Read>(
    anchor: &AbsoluteSystemPath,
    entry: &mut Entry<T>,
) -> Result<AnchoredSystemPathBuf, CacheError> {
    let header = entry.header();

    match header.entry_type() {
        tar::EntryType::Directory => restore_directory(anchor, entry.header()),
        tar::EntryType::Regular => restore_regular(anchor, entry),
        tar::EntryType::Symlink => restore_symlink(anchor, entry.header()),
        ty => Err(CacheError::UnsupportedFileType(ty, Backtrace::capture())),
    }
}

pub fn canonicalize_name(name: &Path) -> Result<AnchoredSystemPathBuf, CacheError> {
    #[allow(unused_variables)]
    let PathValidation {
        well_formed,
        windows_safe,
    } = check_name(name);

    if !well_formed {
        return Err(CacheError::MalformedName(
            name.to_string_lossy().to_string(),
            Backtrace::capture(),
        ));
    }

    #[cfg(windows)]
    {
        if !windows_safe {
            return Err(CacheError::WindowsUnsafeName(
                name.to_string(),
                Backtrace::capture(),
            ));
        }
    }

    // There's no easier way to remove trailing slashes in Rust
    // because `OsString`s are really just `Vec<u8>`s.
    let no_trailing_slash: PathBuf = name.components().collect();

    // We know this is indeed anchored because of `check_name`,
    // and it is indeed system because we just split and combined with the
    // system path separator above
    Ok(AnchoredSystemPathBuf::from_path_buf(no_trailing_slash)?)
}

#[derive(Debug, PartialEq)]
struct PathValidation {
    well_formed: bool,
    windows_safe: bool,
}

fn check_name(name: &Path) -> PathValidation {
    if name.as_os_str().is_empty() {
        return PathValidation {
            well_formed: false,
            windows_safe: false,
        };
    }

    let mut well_formed = true;
    let mut windows_safe = true;
    let name = name.to_string_lossy();
    // Name is:
    // - "."
    // - ".."
    if well_formed && (name == "." || name == "..") {
        well_formed = false;
    }

    // Name starts with:
    // - `/`
    // - `./`
    // - `../`
    if well_formed && (name.starts_with("/") || name.starts_with("./") || name.starts_with("../")) {
        well_formed = false;
    }

    // Name ends in:
    // - `/.`
    // - `/..`
    if well_formed && (name.ends_with("/.") || name.ends_with("/..")) {
        well_formed = false;
    }

    // Name contains:
    // - `//`
    // - `/./`
    // - `/../`
    if well_formed && (name.contains("//") || name.contains("/./") || name.contains("/../")) {
        well_formed = false;
    }

    // Name contains: `\`
    if name.contains('\\') {
        windows_safe = false;
    }

    PathValidation {
        well_formed,
        windows_safe,
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, fs::File, io::empty, path::Path};

    use anyhow::Result;
    use tar::Header;
    use tempfile::{tempdir, TempDir};
    use test_case::test_case;
    use tracing::debug;
    use turbopath::{AbsoluteSystemPath, AbsoluteSystemPathBuf, AnchoredSystemPathBuf};

    use crate::cache_archive::{
        restore::{canonicalize_name, check_name, CacheReader, PathValidation},
        restore_symlink::canonicalize_linkname,
    };

    // Expected output of the cache
    #[derive(Debug)]
    struct ExpectedOutput(Vec<AnchoredSystemPathBuf>);

    enum TarFile {
        File {
            body: Vec<u8>,
            path: AnchoredSystemPathBuf,
        },
        Directory {
            path: AnchoredSystemPathBuf,
        },
        Symlink {
            // The path of the symlink itself
            link_path: AnchoredSystemPathBuf,
            // The target of the symlink
            link_target: AnchoredSystemPathBuf,
        },
        Fifo {
            path: AnchoredSystemPathBuf,
        },
    }

    struct TestCase {
        #[allow(dead_code)]
        name: &'static str,
        // The files we start with
        input_files: Vec<TarFile>,
        // The expected files (there will be more files than `expected_output`
        // since we want to check entries of symlinked directories)
        expected_files: Vec<TarFile>,
        // What we expect to get from CacheArchive::restore, either a
        // Vec of restored files, or an error (represented as a string)
        expected_output: Result<Vec<AnchoredSystemPathBuf>, String>,
    }

    fn generate_tar(test_dir: &TempDir, files: &[TarFile]) -> Result<AbsoluteSystemPathBuf> {
        let test_archive_path = test_dir.path().join("test.tar");
        let archive_file = File::create(&test_archive_path)?;

        let mut tar_writer = tar::Builder::new(archive_file);

        for file in files {
            match file {
                TarFile::File { path, body } => {
                    debug!("Adding file: {:?}", path);
                    let mut header = Header::new_gnu();
                    header.set_size(body.len() as u64);
                    header.set_entry_type(tar::EntryType::Regular);
                    header.set_mode(0o644);
                    tar_writer.append_data(&mut header, path, &body[..])?;
                }
                TarFile::Directory { path } => {
                    debug!("Adding directory: {:?}", path);
                    let mut header = Header::new_gnu();
                    header.set_entry_type(tar::EntryType::Directory);
                    header.set_size(0);
                    header.set_mode(0o755);
                    tar_writer.append_data(&mut header, &path, empty())?;
                }
                TarFile::Symlink {
                    link_path: link_file,
                    link_target,
                } => {
                    debug!("Adding symlink: {:?} -> {:?}", link_file, link_target);
                    let mut header = tar::Header::new_gnu();
                    header.set_username("foo")?;
                    header.set_entry_type(tar::EntryType::Symlink);
                    header.set_size(0);

                    tar_writer.append_link(&mut header, &link_file, &link_target)?;
                }
                // We don't support this, but we need to add it to a tar for testing purposes
                TarFile::Fifo { path } => {
                    let mut header = tar::Header::new_gnu();
                    header.set_entry_type(tar::EntryType::Fifo);
                    header.set_size(0);
                    tar_writer.append_data(&mut header, path, empty())?;
                }
            }
        }

        tar_writer.into_inner()?;

        Ok(AbsoluteSystemPathBuf::new(test_archive_path)?)
    }

    fn compress_tar(archive_path: &AbsoluteSystemPathBuf) -> Result<AbsoluteSystemPathBuf> {
        let mut input_file = File::open(archive_path)?;

        let output_file_path = format!("{}.zst", archive_path.to_str()?);
        let output_file = File::create(&output_file_path)?;

        let mut zw = zstd::stream::Encoder::new(output_file, 0)?;
        std::io::copy(&mut input_file, &mut zw)?;

        zw.finish()?;

        Ok(AbsoluteSystemPathBuf::new(output_file_path)?)
    }

    fn assert_file_exists(anchor: &AbsoluteSystemPath, disk_file: &TarFile) -> Result<()> {
        match disk_file {
            TarFile::File { path, body } => {
                let full_name = anchor.resolve(path);
                debug!("reading {}", full_name.to_string_lossy());
                let file_contents = fs::read(full_name)?;

                assert_eq!(file_contents, *body);
            }
            TarFile::Directory { path } => {
                let full_name = anchor.resolve(path);
                let metadata = fs::metadata(full_name)?;

                assert!(metadata.is_dir());
            }
            TarFile::Symlink {
                link_path: link_file,
                link_target: expected_link_target,
            } => {
                let full_link_file = anchor.resolve(link_file);
                let link_target = fs::read_link(full_link_file)?;

                assert_eq!(link_target, expected_link_target.as_path().to_path_buf());
            }
            TarFile::Fifo { .. } => unreachable!("FIFOs are not supported"),
        }

        Ok(())
    }

    fn into_anchored_system_path_vec(items: Vec<&'static str>) -> Vec<AnchoredSystemPathBuf> {
        items
            .into_iter()
            .map(|item| AnchoredSystemPathBuf::try_from(Path::new(item)).unwrap())
            .collect()
    }

    #[test]
    fn test_name_traversal() -> Result<()> {
        let tar_bytes = include_bytes!("../../fixtures/name-traversal.tar");
        let mut cache_reader = CacheReader::from_reader(&tar_bytes[..], false)?;

        let output_dir = tempdir()?;
        let anchor = AbsoluteSystemPath::new(output_dir.path())?;
        let result = cache_reader.restore(&anchor);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "file name is malformed: ../escape"
        );

        let tar_bytes = include_bytes!("../../fixtures/name-traversal.tar.zst");
        let mut cache_reader = CacheReader::from_reader(&tar_bytes[..], true)?;

        let output_dir = tempdir()?;
        let anchor = AbsoluteSystemPath::new(output_dir.path())?;
        let result = cache_reader.restore(&anchor);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "file name is malformed: ../escape"
        );
        Ok(())
    }

    #[test]
    fn test_restore() -> Result<()> {
        let tests = vec![
            TestCase {
                name: "cache optimized",
                input_files: vec![
                    TarFile::Directory {
                        path: AnchoredSystemPathBuf::from_path_buf("one/")?,
                    },
                    TarFile::Directory {
                        path: AnchoredSystemPathBuf::from_path_buf("one/two/")?,
                    },
                    TarFile::Directory {
                        path: AnchoredSystemPathBuf::from_path_buf("one/two/three/")?,
                    },
                    TarFile::Directory {
                        path: AnchoredSystemPathBuf::from_path_buf("one/two/a/")?,
                    },
                    TarFile::File {
                        body: vec![],
                        path: AnchoredSystemPathBuf::from_path_buf("one/two/three/file-one")?,
                    },
                    TarFile::File {
                        body: vec![],
                        path: AnchoredSystemPathBuf::from_path_buf("one/two/three/file-two")?,
                    },
                    TarFile::File {
                        body: vec![],
                        path: AnchoredSystemPathBuf::from_path_buf("one/two/a/file")?,
                    },
                    TarFile::Directory {
                        path: AnchoredSystemPathBuf::from_path_buf("one/two/b/")?,
                    },
                    TarFile::File {
                        body: vec![],
                        path: AnchoredSystemPathBuf::from_path_buf("one/two/b/file")?,
                    },
                ],
                expected_files: vec![
                    TarFile::Directory {
                        path: AnchoredSystemPathBuf::from_path_buf("one/")?,
                    },
                    TarFile::Directory {
                        path: AnchoredSystemPathBuf::from_path_buf("one/two/")?,
                    },
                    TarFile::Directory {
                        path: AnchoredSystemPathBuf::from_path_buf("one/two/three/")?,
                    },
                    TarFile::File {
                        path: AnchoredSystemPathBuf::from_path_buf("one/two/three/file-one")?,
                        body: vec![],
                    },
                    TarFile::File {
                        path: AnchoredSystemPathBuf::from_path_buf("one/two/three/file-two")?,
                        body: vec![],
                    },
                    TarFile::Directory {
                        path: AnchoredSystemPathBuf::from_path_buf("one/two/a/")?,
                    },
                    TarFile::File {
                        path: AnchoredSystemPathBuf::from_path_buf("one/two/a/file")?,
                        body: vec![],
                    },
                    TarFile::Directory {
                        path: AnchoredSystemPathBuf::from_path_buf("one/two/b/")?,
                    },
                    TarFile::File {
                        path: AnchoredSystemPathBuf::from_path_buf("one/two/b/file")?,
                        body: vec![],
                    },
                ],
                expected_output: Ok(into_anchored_system_path_vec(vec![
                    "one",
                    "one/two",
                    "one/two/three",
                    "one/two/a",
                    "one/two/three/file-one",
                    "one/two/three/file-two",
                    "one/two/a/file",
                    "one/two/b",
                    "one/two/b/file",
                ])),
            },
            TestCase {
                name: "pathological cache works",
                input_files: vec![
                    TarFile::Directory {
                        path: AnchoredSystemPathBuf::from_path_buf("one/")?,
                    },
                    TarFile::Directory {
                        path: AnchoredSystemPathBuf::from_path_buf("one/two/")?,
                    },
                    TarFile::Directory {
                        path: AnchoredSystemPathBuf::from_path_buf("one/two/a/")?,
                    },
                    TarFile::Directory {
                        path: AnchoredSystemPathBuf::from_path_buf("one/two/b/")?,
                    },
                    TarFile::Directory {
                        path: AnchoredSystemPathBuf::from_path_buf("one/two/three/")?,
                    },
                    TarFile::File {
                        body: vec![],
                        path: AnchoredSystemPathBuf::from_path_buf("one/two/a/file")?,
                    },
                    TarFile::File {
                        body: vec![],
                        path: AnchoredSystemPathBuf::from_path_buf("one/two/b/file")?,
                    },
                    TarFile::File {
                        body: vec![],
                        path: AnchoredSystemPathBuf::from_path_buf("one/two/three/file-one")?,
                    },
                    TarFile::File {
                        body: vec![],
                        path: AnchoredSystemPathBuf::from_path_buf("one/two/three/file-two")?,
                    },
                ],
                expected_files: vec![
                    TarFile::Directory {
                        path: AnchoredSystemPathBuf::from_path_buf("one/")?,
                    },
                    TarFile::Directory {
                        path: AnchoredSystemPathBuf::from_path_buf("one/two/")?,
                    },
                    TarFile::Directory {
                        path: AnchoredSystemPathBuf::from_path_buf("one/two/three/")?,
                    },
                    TarFile::File {
                        path: AnchoredSystemPathBuf::from_path_buf("one/two/three/file-one")?,

                        body: vec![],
                    },
                    TarFile::File {
                        path: AnchoredSystemPathBuf::from_path_buf("one/two/three/file-two")?,
                        body: vec![],
                    },
                    TarFile::Directory {
                        path: AnchoredSystemPathBuf::from_path_buf("one/two/a/")?,
                    },
                    TarFile::File {
                        path: AnchoredSystemPathBuf::from_path_buf("one/two/a/file")?,
                        body: vec![],
                    },
                    TarFile::Directory {
                        path: AnchoredSystemPathBuf::from_path_buf("one/two/b/")?,
                    },
                    TarFile::File {
                        path: AnchoredSystemPathBuf::from_path_buf("one/two/b/file")?,
                        body: vec![],
                    },
                ],
                expected_output: Ok(into_anchored_system_path_vec(vec![
                    "one",
                    "one/two",
                    "one/two/a",
                    "one/two/b",
                    "one/two/three",
                    "one/two/a/file",
                    "one/two/b/file",
                    "one/two/three/file-one",
                    "one/two/three/file-two",
                ])),
            },
            TestCase {
                name: "symlink hello world",
                input_files: vec![
                    TarFile::Directory {
                        path: AnchoredSystemPathBuf::from_path_buf("target")?,
                    },
                    TarFile::Symlink {
                        link_path: AnchoredSystemPathBuf::from_path_buf("source")?,
                        link_target: AnchoredSystemPathBuf::from_path_buf("target")?,
                    },
                ],
                expected_files: vec![
                    TarFile::Symlink {
                        link_path: AnchoredSystemPathBuf::from_path_buf("source")?,
                        link_target: AnchoredSystemPathBuf::from_path_buf("target")?,
                    },
                    TarFile::Directory {
                        path: AnchoredSystemPathBuf::from_path_buf("target")?,
                    },
                ],
                expected_output: Ok(into_anchored_system_path_vec(vec!["target", "source"])),
            },
            TestCase {
                name: "nested file",
                input_files: vec![
                    TarFile::Directory {
                        path: AnchoredSystemPathBuf::from_path_buf("folder/")?,
                    },
                    TarFile::File {
                        body: b"file".to_vec(),
                        path: AnchoredSystemPathBuf::from_path_buf("folder/file")?,
                    },
                ],
                expected_files: vec![
                    TarFile::Directory {
                        path: AnchoredSystemPathBuf::from_path_buf("folder/")?,
                    },
                    TarFile::File {
                        path: AnchoredSystemPathBuf::from_path_buf("folder/file")?,
                        body: b"file".to_vec(),
                    },
                ],
                expected_output: Ok(into_anchored_system_path_vec(vec!["folder", "folder/file"])),
            },
            TestCase {
                name: "nested symlink",
                input_files: vec![
                    TarFile::Directory {
                        path: AnchoredSystemPathBuf::from_path_buf("folder/")?,
                    },
                    TarFile::Symlink {
                        link_path: AnchoredSystemPathBuf::from_path_buf("folder/symlink")?,
                        link_target: AnchoredSystemPathBuf::from_path_buf("../")?,
                    },
                    TarFile::File {
                        path: AnchoredSystemPathBuf::from_path_buf(
                            "folder/symlink/folder-sibling",
                        )?,
                        body: b"folder-sibling".to_vec(),
                    },
                ],
                expected_files: vec![
                    TarFile::Directory {
                        path: AnchoredSystemPathBuf::from_path_buf("folder/")?,
                    },
                    TarFile::Symlink {
                        link_path: AnchoredSystemPathBuf::from_path_buf("folder/symlink")?,
                        link_target: AnchoredSystemPathBuf::from_path_buf("../")?,
                    },
                    TarFile::File {
                        path: AnchoredSystemPathBuf::from_path_buf(
                            "folder/symlink/folder-sibling",
                        )?,
                        body: b"folder-sibling".to_vec(),
                    },
                    TarFile::File {
                        path: AnchoredSystemPathBuf::from_path_buf("folder-sibling")?,
                        body: b"folder-sibling".to_vec(),
                    },
                ],
                expected_output: Ok(into_anchored_system_path_vec(vec![
                    "folder",
                    "folder/symlink",
                    "folder/symlink/folder-sibling",
                ])),
            },
            TestCase {
                name: "pathological symlinks",
                input_files: vec![
                    TarFile::Symlink {
                        link_path: AnchoredSystemPathBuf::from_path_buf("one")?,
                        link_target: AnchoredSystemPathBuf::from_path_buf("two")?,
                    },
                    TarFile::Symlink {
                        link_path: AnchoredSystemPathBuf::from_path_buf("two")?,
                        link_target: AnchoredSystemPathBuf::from_path_buf("three")?,
                    },
                    TarFile::Symlink {
                        link_path: AnchoredSystemPathBuf::from_path_buf("three")?,
                        link_target: AnchoredSystemPathBuf::from_path_buf("real")?,
                    },
                    TarFile::File {
                        body: b"real".to_vec(),
                        path: AnchoredSystemPathBuf::from_path_buf("real")?,
                    },
                ],
                expected_files: vec![
                    TarFile::Symlink {
                        link_path: AnchoredSystemPathBuf::from_path_buf("one")?,
                        link_target: AnchoredSystemPathBuf::from_path_buf("two")?,
                    },
                    TarFile::Symlink {
                        link_path: AnchoredSystemPathBuf::from_path_buf("two")?,
                        link_target: AnchoredSystemPathBuf::from_path_buf("three")?,
                    },
                    TarFile::Symlink {
                        link_path: AnchoredSystemPathBuf::from_path_buf("three")?,
                        link_target: AnchoredSystemPathBuf::from_path_buf("real")?,
                    },
                    TarFile::File {
                        path: AnchoredSystemPathBuf::from_path_buf("real")?,
                        body: b"real".to_vec(),
                    },
                ],
                expected_output: Ok(into_anchored_system_path_vec(vec![
                    "real", "one", "two", "three",
                ])),
            },
            TestCase {
                name: "place file at dir location",
                input_files: vec![
                    TarFile::Directory {
                        path: AnchoredSystemPathBuf::from_path_buf("folder-not-file/")?,
                    },
                    TarFile::File {
                        body: b"subfile".to_vec(),
                        path: AnchoredSystemPathBuf::from_path_buf("folder-not-file/subfile")?,
                    },
                    TarFile::File {
                        body: b"this shouldn't work".to_vec(),
                        path: AnchoredSystemPathBuf::from_path_buf("folder-not-file")?,
                    },
                ],

                expected_files: vec![
                    TarFile::Directory {
                        path: AnchoredSystemPathBuf::from_path_buf("folder-not-file/")?,
                    },
                    TarFile::File {
                        body: b"subfile".to_vec(),
                        path: AnchoredSystemPathBuf::from_path_buf("folder-not-file/subfile")?,
                    },
                ],
                expected_output: Err("IO error: Is a directory (os error 21)".to_string()),
            },
            TestCase {
                name: "symlink cycle",
                input_files: vec![
                    TarFile::Symlink {
                        link_path: AnchoredSystemPathBuf::from_path_buf("one")?,
                        link_target: AnchoredSystemPathBuf::from_path_buf("two")?,
                    },
                    TarFile::Symlink {
                        link_path: AnchoredSystemPathBuf::from_path_buf("two")?,
                        link_target: AnchoredSystemPathBuf::from_path_buf("three")?,
                    },
                    TarFile::Symlink {
                        link_path: AnchoredSystemPathBuf::from_path_buf("three")?,
                        link_target: AnchoredSystemPathBuf::from_path_buf("one")?,
                    },
                ],
                expected_files: vec![],
                expected_output: Err("links in the cache are cyclic".to_string()),
            },
            TestCase {
                name: "symlink clobber",
                input_files: vec![
                    TarFile::Symlink {
                        link_path: AnchoredSystemPathBuf::from_path_buf("one")?,
                        link_target: AnchoredSystemPathBuf::from_path_buf("two")?,
                    },
                    TarFile::Symlink {
                        link_path: AnchoredSystemPathBuf::from_path_buf("one")?,
                        link_target: AnchoredSystemPathBuf::from_path_buf("three")?,
                    },
                    TarFile::Symlink {
                        link_path: AnchoredSystemPathBuf::from_path_buf("one")?,
                        link_target: AnchoredSystemPathBuf::from_path_buf("real")?,
                    },
                    TarFile::File {
                        body: b"real".to_vec(),
                        path: AnchoredSystemPathBuf::from_path_buf("real")?,
                    },
                ],
                expected_files: vec![
                    TarFile::Symlink {
                        link_path: AnchoredSystemPathBuf::from_path_buf("one")?,
                        link_target: AnchoredSystemPathBuf::from_path_buf("real")?,
                    },
                    TarFile::File {
                        body: b"real".to_vec(),
                        path: AnchoredSystemPathBuf::from_path_buf("real")?,
                    },
                ],
                expected_output: Ok(into_anchored_system_path_vec(vec!["real", "one"])),
            },
            TestCase {
                name: "symlink traversal",
                input_files: vec![
                    TarFile::Symlink {
                        link_path: AnchoredSystemPathBuf::from_path_buf("escape")?,
                        link_target: AnchoredSystemPathBuf::from_path_buf("../")?,
                    },
                    TarFile::File {
                        body: b"file".to_vec(),
                        path: AnchoredSystemPathBuf::from_path_buf("escape/file")?,
                    },
                ],
                expected_files: vec![TarFile::Symlink {
                    link_path: AnchoredSystemPathBuf::from_path_buf("escape")?,
                    link_target: AnchoredSystemPathBuf::from_path_buf("../")?,
                }],
                expected_output: Err("tar attempts to write outside of directory: ../".to_string()),
            },
            TestCase {
                name: "Double indirection: file",
                input_files: vec![
                    TarFile::Symlink {
                        link_path: AnchoredSystemPathBuf::from_path_buf("up")?,
                        link_target: AnchoredSystemPathBuf::from_path_buf("../")?,
                    },
                    TarFile::Symlink {
                        link_path: AnchoredSystemPathBuf::from_path_buf("link")?,
                        link_target: AnchoredSystemPathBuf::from_path_buf("up")?,
                    },
                    TarFile::File {
                        body: b"file".to_vec(),
                        path: AnchoredSystemPathBuf::from_path_buf("link/outside-file")?,
                    },
                ],
                expected_files: vec![],
                expected_output: Err("tar attempts to write outside of directory: ../".to_string()),
            },
            TestCase {
                name: "Double indirection: folder",
                input_files: vec![
                    TarFile::Symlink {
                        link_path: AnchoredSystemPathBuf::from_path_buf("up")?,
                        link_target: AnchoredSystemPathBuf::from_path_buf("../")?,
                    },
                    TarFile::Symlink {
                        link_path: AnchoredSystemPathBuf::from_path_buf("link")?,
                        link_target: AnchoredSystemPathBuf::from_path_buf("up")?,
                    },
                    TarFile::Directory {
                        path: AnchoredSystemPathBuf::from_path_buf("link/level-one/level-two")?,
                    },
                ],
                expected_files: vec![],
                expected_output: Err("tar attempts to write outside of directory: ../".to_string()),
            },
            TestCase {
                name: "windows unsafe",
                input_files: vec![TarFile::File {
                    body: b"file".to_vec(),
                    path: AnchoredSystemPathBuf::from_path_buf("back\\slash\\file")?,
                }],
                expected_files: {
                    #[cfg(unix)]
                    {
                        vec![TarFile::File {
                            body: b"file".to_vec(),
                            path: AnchoredSystemPathBuf::from_path_buf("back\\slash\\file")?,
                        }]
                    }
                    #[cfg(windows)]
                    vec![]
                },
                #[cfg(unix)]
                expected_output: Ok(into_anchored_system_path_vec(vec!["back\\slash\\file"])),
                #[cfg(windows)]
                expected_output: Err("file name is not Windows-safe".to_string()),
            },
            TestCase {
                name: "fifo (and others) unsupported",
                input_files: vec![TarFile::Fifo {
                    path: AnchoredSystemPathBuf::from_path_buf("fifo")?,
                }],
                expected_files: vec![],
                expected_output: Err("attempted to restore unsupported file type: Fifo".to_string()),
            },
            TestCase {
                name: "duplicate restores",
                input_files: vec![
                    TarFile::File {
                        body: b"target".to_vec(),
                        path: AnchoredSystemPathBuf::from_path_buf("target")?,
                    },
                    TarFile::Symlink {
                        link_path: AnchoredSystemPathBuf::from_path_buf("source")?,
                        link_target: AnchoredSystemPathBuf::from_path_buf("target")?,
                    },
                    TarFile::Directory {
                        path: AnchoredSystemPathBuf::from_path_buf("one/")?,
                    },
                    TarFile::Directory {
                        path: AnchoredSystemPathBuf::from_path_buf("one/two/")?,
                    },
                ],
                expected_files: vec![
                    TarFile::File {
                        body: b"target".to_vec(),
                        path: AnchoredSystemPathBuf::from_path_buf("target")?,
                    },
                    TarFile::Directory {
                        path: AnchoredSystemPathBuf::from_path_buf("one/")?,
                    },
                    TarFile::Directory {
                        path: AnchoredSystemPathBuf::from_path_buf("one/two/")?,
                    },
                    TarFile::Symlink {
                        link_path: AnchoredSystemPathBuf::from_path_buf("source")?,
                        link_target: AnchoredSystemPathBuf::from_path_buf("target")?,
                    },
                ],
                expected_output: Ok(into_anchored_system_path_vec(vec![
                    "target", "source", "one", "one/two",
                ])),
            },
        ];

        for is_compressed in [true, false] {
            for test in &tests {
                let input_dir = tempdir()?;
                let archive_path = generate_tar(&input_dir, &test.input_files)?;
                let output_dir = tempdir()?;
                let anchor = AbsoluteSystemPath::new(output_dir.path())?;

                let archive_path = if is_compressed {
                    compress_tar(&archive_path)?
                } else {
                    archive_path
                };

                let mut cache_reader = CacheReader::open(&archive_path)?;

                match (cache_reader.restore(&anchor), &test.expected_output) {
                    (Ok(restored_files), Err(expected_error)) => {
                        panic!(
                            "expected error: {:?}, received {:?}",
                            expected_error, restored_files
                        );
                    }
                    (Ok(restored_files), Ok(expected_files)) => {
                        assert_eq!(&restored_files, expected_files);
                    }
                    (Err(err), Err(expected_error)) => {
                        assert_eq!(&err.to_string(), expected_error);
                        continue;
                    }
                    (Err(err), Ok(_)) => {
                        panic!("unexpected error: {:?}", err);
                    }
                };

                let expected_files = &test.expected_files;

                for expected_file in expected_files {
                    assert_file_exists(anchor, &expected_file)?;
                }
            }
        }

        Ok(())
    }

    #[test_case("", PathValidation { well_formed: false, windows_safe: false } ; "1")]
    #[test_case(".", PathValidation { well_formed: false, windows_safe: true } ; "2")]
    #[test_case("..", PathValidation { well_formed: false, windows_safe: true } ; "3")]
    #[test_case("/", PathValidation { well_formed: false, windows_safe: true } ; "4")]
    #[test_case("./", PathValidation { well_formed: false, windows_safe: true } ; "5")]
    #[test_case("../", PathValidation { well_formed: false, windows_safe: true } ; "6")]
    #[test_case("/a", PathValidation { well_formed: false, windows_safe: true } ; "7")]
    #[test_case("./a", PathValidation { well_formed: false, windows_safe: true } ; "8")]
    #[test_case("../a", PathValidation { well_formed: false, windows_safe: true } ; "9")]
    #[test_case("/.", PathValidation { well_formed: false, windows_safe: true } ; "10")]
    #[test_case("/..", PathValidation { well_formed: false, windows_safe: true } ; "11")]
    #[test_case("a/.", PathValidation { well_formed: false, windows_safe: true } ; "12")]
    #[test_case("a/..", PathValidation { well_formed: false, windows_safe: true } ; "13")]
    #[test_case("//", PathValidation { well_formed: false, windows_safe: true } ; "14")]
    #[test_case("/./", PathValidation { well_formed: false, windows_safe: true } ; "15")]
    #[test_case("/../", PathValidation { well_formed: false, windows_safe: true } ; "16")]
    #[test_case("a//", PathValidation { well_formed: false, windows_safe: true } ; "17")]
    #[test_case("a/./", PathValidation { well_formed: false, windows_safe: true } ; "18")]
    #[test_case("a/../", PathValidation { well_formed: false, windows_safe: true } ; "19")]
    #[test_case("//a", PathValidation { well_formed: false, windows_safe: true } ; "20")]
    #[test_case("/./a", PathValidation { well_formed: false, windows_safe: true } ; "21")]
    #[test_case("/../a", PathValidation { well_formed: false, windows_safe: true } ; "22")]
    #[test_case("a//a", PathValidation { well_formed: false, windows_safe: true } ; "23")]
    #[test_case("a/./a", PathValidation { well_formed: false, windows_safe: true } ; "24")]
    #[test_case("a/../a", PathValidation { well_formed: false, windows_safe: true } ; "25")]
    #[test_case("...", PathValidation { well_formed: true, windows_safe: true } ; "26")]
    #[test_case(".../a", PathValidation { well_formed: true, windows_safe: true } ; "27")]
    #[test_case("a/...", PathValidation { well_formed: true, windows_safe: true } ; "28")]
    #[test_case("a/.../a", PathValidation { well_formed: true, windows_safe: true } ; "29")]
    #[test_case(".../...", PathValidation { well_formed: true, windows_safe: true } ; "30")]
    fn test_check_name(path: &'static str, expected_output: PathValidation) -> Result<()> {
        let output = check_name(Path::new(path));
        assert_eq!(output, expected_output);

        Ok(())
    }

    #[test_case(Path::new("source").try_into()?, Path::new("target"), "path/to/anchor/target", "path\\to\\anchor\\target" ; "hello world")]
    #[test_case(Path::new("child/source").try_into()?, Path::new("../sibling/target"), "path/to/anchor/sibling/target", "path\\to\\anchor\\sibling\\target" ; "Unix path subdirectory traversal")]
    #[test_case(Path::new("child/source").try_into()?, Path::new("..\\sibling\\target"), "path/to/anchor/child/..\\sibling\\target", "path\\to\\anchor\\sibling\\target" ; "Windows path subdirectory traversal")]
    fn test_canonicalize_linkname(
        processed_name: AnchoredSystemPathBuf,
        linkname: &Path,
        canonical_unix: &'static str,
        #[allow(unused_variables)] canonical_windows: &'static str,
    ) -> Result<()> {
        // Doesn't really matter if this is relative in this case, we just need the type
        // to agree.
        let anchor = unsafe { AbsoluteSystemPath::new_unchecked("path/to/anchor") };

        let received_path = canonicalize_linkname(anchor, &processed_name, linkname)?;

        #[cfg(unix)]
        assert_eq!(received_path.to_string_lossy(), canonical_unix);
        #[cfg(windows)]
        assert_eq!(received_path.to_string_lossy(), canonical_windows);

        Ok(())
    }

    #[test_case(Path::new("test.txt"), Ok("test.txt") ; "hello world")]
    #[test_case(Path::new("something/"), Ok("something") ; "directory")]
    #[test_case(Path::new("//"), Err("file name is malformed: //".to_string()) ; "malformed name")]
    fn test_canonicalize_name(
        file_name: &Path,
        expected: Result<&'static str, String>,
    ) -> Result<()> {
        let result = canonicalize_name(file_name).map_err(|e| e.to_string());
        match (result, expected) {
            (Ok(result), Ok(expected)) => {
                assert_eq!(result.to_str()?, expected)
            }
            (Err(result), Err(expected)) => assert_eq!(result, expected),
            (result, expected) => panic!("Expected {:?}, got {:?}", expected, result),
        }

        Ok(())
    }
}
