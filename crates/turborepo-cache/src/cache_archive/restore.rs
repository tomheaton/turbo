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
use turbopath::{
    AbsoluteSystemPath, AbsoluteSystemPathBuf, AnchoredSystemPathBuf, RelativeSystemPathBuf,
};

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

struct CacheReader {
    reader: Box<dyn Read>,
}

impl CacheReader {
    #[cfg(test)]
    pub fn new(reader: impl Read + 'static, is_compressed: bool) -> Result<Self, CacheError> {
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

    fn restore_entries<'a, T: Read>(
        tr: &'a mut tar::Archive<T>,
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

    fn topologically_restore_symlinks<'a, T: Read>(
        anchor: &AbsoluteSystemPath,
        symlinks: &[Entry<'a, T>],
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

    Ok(RelativeSystemPathBuf::new(no_trailing_slash)?.into())
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
    use std::{collections::HashSet, fs, fs::File, io::empty, path::Path};

    use anyhow::Result;
    use tar::Header;
    use tempfile::{tempdir, TempDir};
    use test_case::test_case;
    use tracing::debug;
    use turbopath::{
        AbsoluteSystemPath, AbsoluteSystemPathBuf, AnchoredSystemPathBuf, RelativeSystemPathBuf,
    };

    use crate::{
        cache_archive::{
            restore::{canonicalize_name, check_name, CacheReader, PathValidation},
            restore_symlink::canonicalize_linkname,
        },
        http::HttpCache,
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
        let mut cache_reader = CacheReader::new(&tar_bytes[..], false)?;
        let output_dir = tempdir()?;
        let anchor = AbsoluteSystemPath::new(output_dir.path())?;
        let result = cache_reader.restore(&anchor);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "file name is malformed: ../escape"
        );

        let tar_bytes = include_bytes!("../../fixtures/name-traversal.tar.zst");
        let mut cache_reader = CacheReader::new(&tar_bytes[..], true)?;
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
                        path: RelativeSystemPathBuf::new("one/")?.into(),
                    },
                    TarFile::Directory {
                        path: RelativeSystemPathBuf::new("one/two/")?.into(),
                    },
                    TarFile::Directory {
                        path: RelativeSystemPathBuf::new("one/two/three/")?.into(),
                    },
                    TarFile::Directory {
                        path: RelativeSystemPathBuf::new("one/two/a/")?.into(),
                    },
                    TarFile::File {
                        body: vec![],
                        path: RelativeSystemPathBuf::new("one/two/three/file-one")?.into(),
                    },
                    TarFile::File {
                        body: vec![],
                        path: RelativeSystemPathBuf::new("one/two/three/file-two")?.into(),
                    },
                    TarFile::File {
                        body: vec![],
                        path: RelativeSystemPathBuf::new("one/two/a/file")?.into(),
                    },
                    TarFile::Directory {
                        path: RelativeSystemPathBuf::new("one/two/b/")?.into(),
                    },
                    TarFile::File {
                        body: vec![],
                        path: RelativeSystemPathBuf::new("one/two/b/file")?.into(),
                    },
                ],
                expected_files: vec![
                    TarFile::Directory {
                        path: RelativeSystemPathBuf::new("one/")?.into(),
                    },
                    TarFile::Directory {
                        path: RelativeSystemPathBuf::new("one/two/")?.into(),
                    },
                    TarFile::Directory {
                        path: RelativeSystemPathBuf::new("one/two/three/")?.into(),
                    },
                    TarFile::File {
                        path: RelativeSystemPathBuf::new("one/two/three/file-one")?.into(),
                        body: vec![],
                    },
                    TarFile::File {
                        path: RelativeSystemPathBuf::new("one/two/three/file-two")?.into(),
                        body: vec![],
                    },
                    TarFile::Directory {
                        path: RelativeSystemPathBuf::new("one/two/a/")?.into(),
                    },
                    TarFile::File {
                        path: RelativeSystemPathBuf::new("one/two/a/file")?.into(),
                        body: vec![],
                    },
                    TarFile::Directory {
                        path: RelativeSystemPathBuf::new("one/two/b/")?.into(),
                    },
                    TarFile::File {
                        path: RelativeSystemPathBuf::new("one/two/b/file")?.into(),
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
                        path: RelativeSystemPathBuf::new("one/")?.into(),
                    },
                    TarFile::Directory {
                        path: RelativeSystemPathBuf::new("one/two/")?.into(),
                    },
                    TarFile::Directory {
                        path: RelativeSystemPathBuf::new("one/two/a/")?.into(),
                    },
                    TarFile::Directory {
                        path: RelativeSystemPathBuf::new("one/two/b/")?.into(),
                    },
                    TarFile::Directory {
                        path: RelativeSystemPathBuf::new("one/two/three/")?.into(),
                    },
                    TarFile::File {
                        body: vec![],
                        path: RelativeSystemPathBuf::new("one/two/a/file")?.into(),
                    },
                    TarFile::File {
                        body: vec![],
                        path: RelativeSystemPathBuf::new("one/two/b/file")?.into(),
                    },
                    TarFile::File {
                        body: vec![],
                        path: RelativeSystemPathBuf::new("one/two/three/file-one")?.into(),
                    },
                    TarFile::File {
                        body: vec![],
                        path: RelativeSystemPathBuf::new("one/two/three/file-two")?.into(),
                    },
                ],
                expected_files: vec![
                    TarFile::Directory {
                        path: RelativeSystemPathBuf::new("one/")?.into(),
                    },
                    TarFile::Directory {
                        path: RelativeSystemPathBuf::new("one/two/")?.into(),
                    },
                    TarFile::Directory {
                        path: RelativeSystemPathBuf::new("one/two/three/")?.into(),
                    },
                    TarFile::File {
                        path: RelativeSystemPathBuf::new("one/two/three/file-one")?.into(),
                        body: vec![],
                    },
                    TarFile::File {
                        path: RelativeSystemPathBuf::new("one/two/three/file-two")?.into(),
                        body: vec![],
                    },
                    TarFile::Directory {
                        path: RelativeSystemPathBuf::new("one/two/a/")?.into(),
                    },
                    TarFile::File {
                        path: RelativeSystemPathBuf::new("one/two/a/file")?.into(),
                        body: vec![],
                    },
                    TarFile::Directory {
                        path: RelativeSystemPathBuf::new("one/two/b/")?.into(),
                    },
                    TarFile::File {
                        path: RelativeSystemPathBuf::new("one/two/b/file")?.into(),
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
                        path: RelativeSystemPathBuf::new("target")?.into(),
                    },
                    TarFile::Symlink {
                        link_path: RelativeSystemPathBuf::new("source")?.into(),
                        link_target: RelativeSystemPathBuf::new("target")?.into(),
                    },
                ],
                expected_files: vec![
                    TarFile::Symlink {
                        link_path: RelativeSystemPathBuf::new("source")?.into(),
                        link_target: RelativeSystemPathBuf::new("target")?.into(),
                    },
                    TarFile::Directory {
                        path: RelativeSystemPathBuf::new("target")?.into(),
                    },
                ],
                expected_output: Ok(into_anchored_system_path_vec(vec!["target", "source"])),
            },
            TestCase {
                name: "nested file",
                input_files: vec![
                    TarFile::Directory {
                        path: RelativeSystemPathBuf::new("folder/")?.into(),
                    },
                    TarFile::File {
                        body: b"file".to_vec(),
                        path: RelativeSystemPathBuf::new("folder/file")?.into(),
                    },
                ],
                expected_files: vec![
                    TarFile::Directory {
                        path: RelativeSystemPathBuf::new("folder/")?.into(),
                    },
                    TarFile::File {
                        path: RelativeSystemPathBuf::new("folder/file")?.into(),
                        body: b"file".to_vec(),
                    },
                ],
                expected_output: Ok(into_anchored_system_path_vec(vec!["folder", "folder/file"])),
            },
            TestCase {
                name: "nested symlink",
                input_files: vec![
                    TarFile::Directory {
                        path: RelativeSystemPathBuf::new("folder/")?.into(),
                    },
                    TarFile::Symlink {
                        link_path: RelativeSystemPathBuf::new("folder/symlink")?.into(),
                        link_target: RelativeSystemPathBuf::new("../")?.into(),
                    },
                    TarFile::File {
                        path: RelativeSystemPathBuf::new("folder/symlink/folder-sibling")?.into(),
                        body: b"folder-sibling".to_vec(),
                    },
                ],
                expected_files: vec![
                    TarFile::Directory {
                        path: RelativeSystemPathBuf::new("folder/")?.into(),
                    },
                    TarFile::Symlink {
                        link_path: RelativeSystemPathBuf::new("folder/symlink")?.into(),
                        link_target: RelativeSystemPathBuf::new("../")?.into(),
                    },
                    TarFile::File {
                        path: RelativeSystemPathBuf::new("folder/symlink/folder-sibling")?.into(),
                        body: b"folder-sibling".to_vec(),
                    },
                    TarFile::File {
                        path: RelativeSystemPathBuf::new("folder-sibling")?.into(),
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
                        link_path: RelativeSystemPathBuf::new("one")?.into(),
                        link_target: RelativeSystemPathBuf::new("two")?.into(),
                    },
                    TarFile::Symlink {
                        link_path: RelativeSystemPathBuf::new("two")?.into(),
                        link_target: RelativeSystemPathBuf::new("three")?.into(),
                    },
                    TarFile::Symlink {
                        link_path: RelativeSystemPathBuf::new("three")?.into(),
                        link_target: RelativeSystemPathBuf::new("real")?.into(),
                    },
                    TarFile::File {
                        body: b"real".to_vec(),
                        path: RelativeSystemPathBuf::new("real")?.into(),
                    },
                ],
                expected_files: vec![
                    TarFile::Symlink {
                        link_path: RelativeSystemPathBuf::new("one")?.into(),
                        link_target: RelativeSystemPathBuf::new("two")?.into(),
                    },
                    TarFile::Symlink {
                        link_path: RelativeSystemPathBuf::new("two")?.into(),
                        link_target: RelativeSystemPathBuf::new("three")?.into(),
                    },
                    TarFile::Symlink {
                        link_path: RelativeSystemPathBuf::new("three")?.into(),
                        link_target: RelativeSystemPathBuf::new("real")?.into(),
                    },
                    TarFile::File {
                        path: RelativeSystemPathBuf::new("real")?.into(),
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
                        path: RelativeSystemPathBuf::new("folder-not-file/")?.into(),
                    },
                    TarFile::File {
                        body: b"subfile".to_vec(),
                        path: RelativeSystemPathBuf::new("folder-not-file/subfile")?.into(),
                    },
                    TarFile::File {
                        body: b"this shouldn't work".to_vec(),
                        path: RelativeSystemPathBuf::new("folder-not-file")?.into(),
                    },
                ],

                expected_files: vec![
                    TarFile::Directory {
                        path: RelativeSystemPathBuf::new("folder-not-file/")?.into(),
                    },
                    TarFile::File {
                        body: b"subfile".to_vec(),
                        path: RelativeSystemPathBuf::new("folder-not-file/subfile")?.into(),
                    },
                ],
                expected_output: Err("IO error: Is a directory (os error 21)".to_string()),
            },
            TestCase {
                name: "symlink cycle",
                input_files: vec![
                    TarFile::Symlink {
                        link_path: RelativeSystemPathBuf::new("one")?.into(),
                        link_target: RelativeSystemPathBuf::new("two")?.into(),
                    },
                    TarFile::Symlink {
                        link_path: RelativeSystemPathBuf::new("two")?.into(),
                        link_target: RelativeSystemPathBuf::new("three")?.into(),
                    },
                    TarFile::Symlink {
                        link_path: RelativeSystemPathBuf::new("three")?.into(),
                        link_target: RelativeSystemPathBuf::new("one")?.into(),
                    },
                ],
                expected_files: vec![],
                expected_output: Err("links in the cache are cyclic".to_string()),
            },
            TestCase {
                name: "symlink clobber",
                input_files: vec![
                    TarFile::Symlink {
                        link_path: RelativeSystemPathBuf::new("one")?.into(),
                        link_target: RelativeSystemPathBuf::new("two")?.into(),
                    },
                    TarFile::Symlink {
                        link_path: RelativeSystemPathBuf::new("one")?.into(),
                        link_target: RelativeSystemPathBuf::new("three")?.into(),
                    },
                    TarFile::Symlink {
                        link_path: RelativeSystemPathBuf::new("one")?.into(),
                        link_target: RelativeSystemPathBuf::new("real")?.into(),
                    },
                    TarFile::File {
                        body: b"real".to_vec(),
                        path: RelativeSystemPathBuf::new("real")?.into(),
                    },
                ],
                expected_files: vec![
                    TarFile::Symlink {
                        link_path: RelativeSystemPathBuf::new("one")?.into(),
                        link_target: RelativeSystemPathBuf::new("real")?.into(),
                    },
                    TarFile::File {
                        body: b"real".to_vec(),
                        path: RelativeSystemPathBuf::new("real")?.into(),
                    },
                ],
                expected_output: Ok(into_anchored_system_path_vec(vec!["real", "one"])),
            },
            TestCase {
                name: "symlink traversal",
                input_files: vec![
                    TarFile::Symlink {
                        link_path: RelativeSystemPathBuf::new("escape")?.into(),
                        link_target: RelativeSystemPathBuf::new("../")?.into(),
                    },
                    TarFile::File {
                        body: b"file".to_vec(),
                        path: RelativeSystemPathBuf::new("escape/file")?.into(),
                    },
                ],
                expected_files: vec![TarFile::Symlink {
                    link_path: RelativeSystemPathBuf::new("escape")?.into(),
                    link_target: RelativeSystemPathBuf::new("../")?.into(),
                }],
                expected_output: Err("tar attempts to write outside of directory: ../".to_string()),
            },
            TestCase {
                name: "Double indirection: file",
                input_files: vec![
                    TarFile::Symlink {
                        link_path: RelativeSystemPathBuf::new("up")?.into(),
                        link_target: RelativeSystemPathBuf::new("../")?.into(),
                    },
                    TarFile::Symlink {
                        link_path: RelativeSystemPathBuf::new("link")?.into(),
                        link_target: RelativeSystemPathBuf::new("up")?.into(),
                    },
                    TarFile::File {
                        body: b"file".to_vec(),
                        path: RelativeSystemPathBuf::new("link/outside-file")?.into(),
                    },
                ],
                expected_files: vec![],
                expected_output: Err("tar attempts to write outside of directory: ../".to_string()),
            },
            TestCase {
                name: "Double indirection: folder",
                input_files: vec![
                    TarFile::Symlink {
                        link_path: RelativeSystemPathBuf::new("up")?.into(),
                        link_target: RelativeSystemPathBuf::new("../")?.into(),
                    },
                    TarFile::Symlink {
                        link_path: RelativeSystemPathBuf::new("link")?.into(),
                        link_target: RelativeSystemPathBuf::new("up")?.into(),
                    },
                    TarFile::Directory {
                        path: RelativeSystemPathBuf::new("link/level-one/level-two")?.into(),
                    },
                ],
                expected_files: vec![],
                expected_output: Err("tar attempts to write outside of directory: ../".to_string()),
            },
            TestCase {
                name: "windows unsafe",
                input_files: vec![TarFile::File {
                    body: b"file".to_vec(),
                    path: RelativeSystemPathBuf::new("back\\slash\\file")?.into(),
                }],
                expected_files: {
                    #[cfg(unix)]
                    {
                        vec![TarFile::File {
                            body: b"file".to_vec(),
                            path: RelativeSystemPathBuf::new("back\\slash\\file")?.into(),
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
                    path: RelativeSystemPathBuf::new("fifo")?.into(),
                }],
                expected_files: vec![],
                expected_output: Err("attempted to restore unsupported file type: Fifo".to_string()),
            },
            TestCase {
                name: "duplicate restores",
                input_files: vec![
                    TarFile::File {
                        body: b"target".to_vec(),
                        path: RelativeSystemPathBuf::new("target")?.into(),
                    },
                    TarFile::Symlink {
                        link_path: RelativeSystemPathBuf::new("source")?.into(),
                        link_target: RelativeSystemPathBuf::new("target")?.into(),
                    },
                    TarFile::Directory {
                        path: RelativeSystemPathBuf::new("one/")?.into(),
                    },
                    TarFile::Directory {
                        path: RelativeSystemPathBuf::new("one/two/")?.into(),
                    },
                ],
                expected_files: vec![
                    TarFile::File {
                        body: b"target".to_vec(),
                        path: RelativeSystemPathBuf::new("target")?.into(),
                    },
                    TarFile::Directory {
                        path: RelativeSystemPathBuf::new("one/")?.into(),
                    },
                    TarFile::Directory {
                        path: RelativeSystemPathBuf::new("one/two/")?.into(),
                    },
                    TarFile::Symlink {
                        link_path: RelativeSystemPathBuf::new("source")?.into(),
                        link_target: RelativeSystemPathBuf::new("target")?.into(),
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
        canonical_windows: &'static str,
    ) -> Result<()> {
        let anchor = unsafe { AbsoluteSystemPath::new_unchecked(Path::new("path/to/anchor")) };

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

    #[derive(Debug)]
    struct TarTestCase {
        expected_entries: HashSet<&'static str>,
        tar_file: &'static [u8],
    }

    fn get_tar_test_cases() -> Vec<TarTestCase> {
        vec![
            TarTestCase {
                expected_entries: vec![
                    "apps/web/.next/",
                    "apps/web/.next/BUILD_ID",
                    "apps/web/.next/build-manifest.json",
                    "apps/web/.next/cache/",
                    "apps/web/.next/export-marker.json",
                    "apps/web/.next/images-manifest.json",
                    "apps/web/.next/next-server.js.nft.json",
                    "apps/web/.next/package.json",
                    "apps/web/.next/prerender-manifest.json",
                    "apps/web/.next/react-loadable-manifest.json",
                    "apps/web/.next/required-server-files.json",
                    "apps/web/.next/routes-manifest.json",
                    "apps/web/.next/server/",
                    "apps/web/.next/server/chunks/",
                    "apps/web/.next/server/chunks/font-manifest.json",
                    "apps/web/.next/server/font-manifest.json",
                    "apps/web/.next/server/middleware-build-manifest.js",
                    "apps/web/.next/server/middleware-manifest.json",
                    "apps/web/.next/server/middleware-react-loadable-manifest.js",
                    "apps/web/.next/server/next-font-manifest.js",
                    "apps/web/.next/server/next-font-manifest.json",
                    "apps/web/.next/server/pages/",
                    "apps/web/.next/server/pages-manifest.json",
                    "apps/web/.next/server/pages/404.html",
                    "apps/web/.next/server/pages/500.html",
                    "apps/web/.next/server/pages/_app.js",
                    "apps/web/.next/server/pages/_app.js.nft.json",
                    "apps/web/.next/server/pages/_document.js",
                    "apps/web/.next/server/pages/_document.js.nft.json",
                    "apps/web/.next/server/pages/_error.js",
                    "apps/web/.next/server/pages/_error.js.nft.json",
                    "apps/web/.next/server/pages/index.html",
                    "apps/web/.next/server/pages/index.js.nft.json",
                    "apps/web/.next/server/webpack-runtime.js",
                    "apps/web/.next/static/",
                    "apps/web/.next/static/cYEK0_ogl5lSUpJIM1e5A/",
                    "apps/web/.next/static/cYEK0_ogl5lSUpJIM1e5A/_buildManifest.js",
                    "apps/web/.next/static/cYEK0_ogl5lSUpJIM1e5A/_ssgManifest.js",
                    "apps/web/.next/static/chunks/",
                    "apps/web/.next/static/chunks/framework-ffffd4e8198d9762.js",
                    "apps/web/.next/static/chunks/main-7595d4b6af4c2f6f.js",
                    "apps/web/.next/static/chunks/pages/",
                    "apps/web/.next/static/chunks/pages/_app-09b371265aa7309d.js",
                    "apps/web/.next/static/chunks/pages/_error-e0615e853f5988ee.js",
                    "apps/web/.next/static/chunks/pages/index-c7599f437e77dab6.js",
                    "apps/web/.next/static/chunks/polyfills-c67a75d1b6f99dc8.js",
                    "apps/web/.next/static/chunks/webpack-4e7214a60fad8e88.js",
                    "apps/web/.next/trace",
                    "apps/web/.turbo/turbo-build.log",
                ]
                .into_iter()
                .collect(),
                tar_file: include_bytes!("../../fixtures/627737318531b1db.tar.zst"),
            },
            TarTestCase {
                expected_entries: vec![
                    "apps/docs/.next/",
                    "apps/docs/.next/BUILD_ID",
                    "apps/docs/.next/build-manifest.json",
                    "apps/docs/.next/cache/",
                    "apps/docs/.next/export-marker.json",
                    "apps/docs/.next/images-manifest.json",
                    "apps/docs/.next/next-server.js.nft.json",
                    "apps/docs/.next/package.json",
                    "apps/docs/.next/prerender-manifest.json",
                    "apps/docs/.next/react-loadable-manifest.json",
                    "apps/docs/.next/required-server-files.json",
                    "apps/docs/.next/routes-manifest.json",
                    "apps/docs/.next/server/",
                    "apps/docs/.next/server/chunks/",
                    "apps/docs/.next/server/chunks/font-manifest.json",
                    "apps/docs/.next/server/font-manifest.json",
                    "apps/docs/.next/server/middleware-build-manifest.js",
                    "apps/docs/.next/server/middleware-manifest.json",
                    "apps/docs/.next/server/middleware-react-loadable-manifest.js",
                    "apps/docs/.next/server/next-font-manifest.js",
                    "apps/docs/.next/server/next-font-manifest.json",
                    "apps/docs/.next/server/pages/",
                    "apps/docs/.next/server/pages-manifest.json",
                    "apps/docs/.next/server/pages/404.html",
                    "apps/docs/.next/server/pages/500.html",
                    "apps/docs/.next/server/pages/_app.js",
                    "apps/docs/.next/server/pages/_app.js.nft.json",
                    "apps/docs/.next/server/pages/_document.js",
                    "apps/docs/.next/server/pages/_document.js.nft.json",
                    "apps/docs/.next/server/pages/_error.js",
                    "apps/docs/.next/server/pages/_error.js.nft.json",
                    "apps/docs/.next/server/pages/index.html",
                    "apps/docs/.next/server/pages/index.js.nft.json",
                    "apps/docs/.next/server/webpack-runtime.js",
                    "apps/docs/.next/static/",
                    "apps/docs/.next/static/3WdlFLztLa3kpcDUT_2Yc/",
                    "apps/docs/.next/static/3WdlFLztLa3kpcDUT_2Yc/_buildManifest.js",
                    "apps/docs/.next/static/3WdlFLztLa3kpcDUT_2Yc/_ssgManifest.js",
                    "apps/docs/.next/static/chunks/",
                    "apps/docs/.next/static/chunks/framework-ffffd4e8198d9762.js",
                    "apps/docs/.next/static/chunks/main-7595d4b6af4c2f6f.js",
                    "apps/docs/.next/static/chunks/pages/",
                    "apps/docs/.next/static/chunks/pages/_app-09b371265aa7309d.js",
                    "apps/docs/.next/static/chunks/pages/_error-e0615e853f5988ee.js",
                    "apps/docs/.next/static/chunks/pages/index-53e8899c12a3a0e1.js",
                    "apps/docs/.next/static/chunks/polyfills-c67a75d1b6f99dc8.js",
                    "apps/docs/.next/static/chunks/webpack-4e7214a60fad8e88.js",
                    "apps/docs/.next/trace",
                    "apps/docs/.turbo/turbo-build.log",
                ]
                .into_iter()
                .collect(),
                tar_file: include_bytes!("../../fixtures/c5da7df87dc01a95.tar.zst"),
            },
        ]
    }

    #[test]
    fn test_restore_tar() -> Result<()> {
        let test_cases = get_tar_test_cases();

        for test_case in test_cases {
            let temp_dir = tempdir()?;
            let files = HttpCache::restore_tar(
                &AbsoluteSystemPathBuf::new(temp_dir.path())?,
                test_case.tar_file,
            )?;

            assert_eq!(files.len(), test_case.expected_entries.len());

            for file in files {
                assert!(test_case
                    .expected_entries
                    .contains(file.as_path().to_str().unwrap()));
            }
        }

        Ok(())
    }
}
