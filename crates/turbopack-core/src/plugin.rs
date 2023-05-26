use turbo_tasks::Vc;

use crate::{asset::Asset, context::AssetContext, resolve::ModulePart};

#[turbo_tasks::value_trait]
pub trait CustomModuleType {
    fn create_module(
        self: Vc<Self>,
        source: Vc<Box<dyn Asset>>,
        context: Vc<Box<dyn AssetContext>>,
        part: Option<Vc<ModulePart>>,
    ) -> Vc<Box<dyn Asset>>;
}
