use anyhow::Result;
use turbo_tasks::{
    graph::{GraphTraversal, NonDeterministic},
    Completion, Completions, Vc,
};

use crate::{asset::Asset, reference::all_referenced_assets};

async fn get_referenced_assets(
    parent: Vc<Box<dyn Asset>>,
) -> Result<impl Iterator<Item = Vc<Box<dyn Asset>>> + Send> {
    Ok(all_referenced_assets(parent)
        .await?
        .clone_value()
        .into_iter())
}

/// Returns a completion that changes when any content of any asset in the whole
/// asset graph changes.
#[turbo_tasks::function]
pub async fn any_content_changed(root: Vc<Box<dyn Asset>>) -> Result<Vc<Completion>> {
    let completions = NonDeterministic::new()
        .skip_duplicates()
        .visit([root], get_referenced_assets)
        .await
        .completed()?
        .into_inner()
        .into_iter()
        .map(content_changed)
        .collect();

    Ok(Vc::<Completions>::cell(completions).completed())
}

/// Returns a completion that changes when the content of the given asset
/// changes.
#[turbo_tasks::function]
pub async fn content_changed(asset: Vc<Box<dyn Asset>>) -> Result<Vc<Completion>> {
    // Reading the file content is enough to add as dependency
    asset.content().file_content().await?;
    Ok(Completion::new())
}
