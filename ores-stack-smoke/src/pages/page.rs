use ores_api_docs_client::{PageContext, PageDocument, PageResult};
use ores_api_docs_macros::ores_page;

#[ores_page(
    renderer = "mash",
    delivery = "ssr_only",
    render = "dynamic",
    title = "Fiducia CLI smoke",
    summary = "Exercises ores-stack CLI admission and route projection",
    data_sources("rpc:Health"),
    tags("fiducia", "ores-stack-smoke")
)]
pub async fn page(_ctx: PageContext) -> PageResult {
    Ok(PageDocument::html("<main>fiducia ores-stack smoke</main>"))
}
