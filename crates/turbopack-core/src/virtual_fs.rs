use anyhow::{bail, Result};
use turbo_tasks::{Completion, ValueToString, Vc};
use turbo_tasks_fs::{
    DirectoryContent, FileContent, FileMeta, FileSystem, FileSystemPath, LinkContent,
};

#[turbo_tasks::value]
pub struct VirtualFileSystem;

#[turbo_tasks::value_impl]
impl VirtualFileSystem {
    #[turbo_tasks::function]
    pub fn new() -> Vc<Self> {
        Vc::<Self>::cell(VirtualFileSystem)
    }
}

#[turbo_tasks::value_impl]
impl FileSystem for VirtualFileSystem {
    #[turbo_tasks::function]
    fn read(&self, _fs_path: Vc<FileSystemPath>) -> Result<Vc<FileContent>> {
        bail!("Reading is not possible on the virtual file system")
    }

    #[turbo_tasks::function]
    fn read_link(&self, _fs_path: Vc<FileSystemPath>) -> Result<Vc<LinkContent>> {
        bail!("Reading is not possible on the virtual file system")
    }

    #[turbo_tasks::function]
    fn read_dir(&self, _fs_path: Vc<FileSystemPath>) -> Result<Vc<DirectoryContent>> {
        bail!("Reading is not possible on the virtual file system")
    }

    #[turbo_tasks::function]
    fn track(&self, _fs_path: Vc<FileSystemPath>) -> Result<Vc<Completion>> {
        bail!("Tracking is not possible on the virtual file system")
    }

    #[turbo_tasks::function]
    fn write(
        &self,
        _fs_path: Vc<FileSystemPath>,
        _content: Vc<FileContent>,
    ) -> Result<Vc<Completion>> {
        bail!("Writing is not possible on the virtual file system")
    }

    #[turbo_tasks::function]
    fn write_link(
        &self,
        _fs_path: Vc<FileSystemPath>,
        _target: Vc<LinkContent>,
    ) -> Result<Vc<Completion>> {
        bail!("Writing is not possible on the virtual file system")
    }

    #[turbo_tasks::function]
    fn metadata(&self, _fs_path: Vc<FileSystemPath>) -> Result<Vc<FileMeta>> {
        bail!("Reading is not possible on the virtual file system")
    }
}

#[turbo_tasks::value_impl]
impl ValueToString for VirtualFileSystem {
    #[turbo_tasks::function]
    fn to_string(&self) -> Vc<String> {
        Vc::cell("virtual file system".to_string())
    }
}
