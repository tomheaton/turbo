use anyhow::{bail, Result};
use turbo_tasks::{Value, ValueToString, Vc};

use super::ChunkableAsset;
use crate::{
    asset::Asset,
    context::AssetContext,
    reference_type::{EntryReferenceSubType, ReferenceType},
};

/// Marker trait for the chunking context to accept evaluated entries.
///
/// The chunking context implementation will resolve the dynamic entry to a
/// well-known value or trait object.
#[turbo_tasks::value_trait]
pub trait EvaluatableAsset: Asset + ChunkableAsset {}

#[turbo_tasks::value_impl]
impl EvaluatableAsset {
    #[turbo_tasks::function]
    pub async fn from_asset(
        asset: Vc<Box<dyn Asset>>,
        context: Vc<Box<dyn AssetContext>>,
    ) -> Result<Vc<EvaluatableAsset>> {
        let asset = context.process(
            asset,
            Value::new(ReferenceType::Entry(EntryReferenceSubType::Runtime)),
        );
        let Some(entry) = Vc::try_resolve_downcast::<EvaluatableAsset>(asset).await? else {
            bail!("{} is not a valid evaluated entry", asset.ident().to_string().await?)
        };
        Ok(entry)
    }
}

#[turbo_tasks::value(transparent)]
pub struct EvaluatableAssets(Vec<Vc<EvaluatableAsset>>);

#[turbo_tasks::value_impl]
impl EvaluatableAssets {
    #[turbo_tasks::function]
    pub fn empty() -> Vc<EvaluatableAssets> {
        EvaluatableAssets(vec![]).cell()
    }

    #[turbo_tasks::function]
    pub fn one(entry: Vc<EvaluatableAsset>) -> Vc<EvaluatableAssets> {
        EvaluatableAssets(vec![entry]).cell()
    }

    #[turbo_tasks::function]
    pub async fn with_entry(
        self: Vc<Self>,
        entry: Vc<EvaluatableAsset>,
    ) -> Result<Vc<EvaluatableAssets>> {
        let mut entries = self.await?.clone_value();
        entries.push(entry);
        Ok(EvaluatableAssets(entries).cell())
    }
}
