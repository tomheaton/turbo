use std::fmt::Debug;

use anyhow::Result;
use turbo_tasks::Vc;
use turbo_tasks_fs::FileSystemPath;

use super::{Chunk, EvaluatableAssets};
use crate::{
    asset::{Asset, Assets},
    environment::Environment,
    ident::AssetIdent,
};

/// A context for the chunking that influences the way chunks are created
#[turbo_tasks::value_trait]
pub trait ChunkingContext {
    fn context_path(&self) -> Vc<FileSystemPath>;
    fn output_root(&self) -> Vc<FileSystemPath>;

    // TODO remove this, a chunking context should not be bound to a specific
    // environment since this can change due to transitions in the module graph
    fn environment(&self) -> Vc<Environment>;

    // TODO(alexkirsz) Remove this from the chunking context. This should be at the
    // discretion of chunking context implementors. However, we currently use this
    // in a couple of places in `turbopack-css`, so we need to remove that
    // dependency first.
    fn chunk_path(&self, ident: Vc<AssetIdent>, extension: &str) -> Vc<FileSystemPath>;

    // TODO(alexkirsz) Remove this from the chunking context.
    /// Reference Source Map Assets for chunks
    fn reference_chunk_source_maps(&self, chunk: Vc<Box<dyn Asset>>) -> Vc<bool>;

    fn can_be_in_same_chunk(
        &self,
        asset_a: Vc<Box<dyn Asset>>,
        asset_b: Vc<Box<dyn Asset>>,
    ) -> Vc<bool>;

    fn asset_path(
        &self,
        content_hash: &str,
        original_asset_ident: Vc<AssetIdent>,
    ) -> Vc<FileSystemPath>;

    fn is_hot_module_replacement_enabled(&self) -> Vc<bool> {
        Vc::cell(false)
    }

    fn layer(&self) -> Vc<String> {
        Vc::cell("".to_string())
    }

    fn with_layer(&self, layer: &str) -> Vc<Box<dyn ChunkingContext>>;

    fn chunk_group(&self, entry: Vc<Box<dyn Chunk>>) -> Vc<Assets>;

    fn evaluated_chunk_group(
        &self,
        entry: Vc<Box<dyn Chunk>>,
        evaluatable_assets: Vc<EvaluatableAssets>,
    ) -> Vc<Assets>;
}
