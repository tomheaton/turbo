use turbo_tasks::Vc;
use turbo_tasks_fs::FileSystemPath;

use crate::{
    asset::{Asset, AssetContent},
    ident::AssetIdent,
};

/// An [Asset] that is created from some passed source code.
#[turbo_tasks::value]
pub struct VirtualAsset {
    pub ident: Vc<AssetIdent>,
    pub content: Vc<AssetContent>,
}

#[turbo_tasks::value_impl]
impl VirtualAsset {
    #[turbo_tasks::function]
    pub fn new(path: Vc<FileSystemPath>, content: Vc<AssetContent>) -> Vc<Self> {
        Self::cell(VirtualAsset {
            ident: AssetIdent::from_path(path),
            content,
        })
    }

    #[turbo_tasks::function]
    pub fn new_with_ident(ident: Vc<AssetIdent>, content: Vc<AssetContent>) -> Vc<Self> {
        Self::cell(VirtualAsset { ident, content })
    }
}

#[turbo_tasks::value_impl]
impl Asset for VirtualAsset {
    #[turbo_tasks::function]
    fn ident(&self) -> Vc<AssetIdent> {
        self.ident
    }

    #[turbo_tasks::function]
    fn content(&self) -> Vc<AssetContent> {
        self.content
    }
}
