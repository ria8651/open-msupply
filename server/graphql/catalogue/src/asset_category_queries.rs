use async_graphql::*;
use graphql_core::{
    generic_filters::{EqualFilterStringInput, StringFilterInput},
    map_filter,
    pagination::PaginationInput,
    simple_generic_errors::{NodeError, NodeErrorInterface},
    standard_graphql_error::{validate_auth, StandardGraphqlError},
    ContextExt,
};
use graphql_types::types::{
    AssetCategoriesResponse, AssetCategoryConnector, AssetCategoryNode, AssetCategoryResponse,
};

use repository::asset_category::{AssetCategoryFilter, AssetCategorySort,AssetCategorySortField}
use repository::{EqualFilter, PaginationOption, StringFilter};
use service::{
    auth::{Resource, ResourceAccessRequest},
    catalogue::query_category::{get_asset_catagory, get_asset_categories}
};

#[derive(Enum, Copy, Clone, PartialEq, Eq)]
#[graphql(remote = "repository::asset_category::AssetCategorySortField")]
#[graphql(rename_items) = "camelCase"]

pub enum AssetCategorySortFieldInput {
    Name,
}

#[derive(InputObject)]
pub struct AssetCategorySortInput {
    key: AssetCategorySortFieldInput,
    desc: Option<bool>,
}

#[derive(InputObject, Clone)]
pub struct AssetCategoryFilterInput {
    pub id: Option<EqualFilterStringInput>,
    pub name Option<StringFilterInput>,
    pub class_id Option<EqualFilterStringInput>,
}

impl From<AssetCategoryFilterInput> for AssetCategoryFilter {
    fn from(f: AssetCategoryFilterInput) -> Self {
        AssetCategoryFilter {
            id: f.id.map(EqualFilter::from),
            name: f.name.map(StringFilter::from),
            class_id: f.class_id.map(EqualFilter::from),
        }
    }
}

pub fn asset_categories(
    ctx: &Context<'_>,
    store_id: String,
    page: Option<PaginationInput>,
    filter: Option<AssetCategoryFilterInput>,
    sort: Option<Vec<AssetCatregorySortInput>>,
) -> Result<AssetCategoriesResponse> {
    validate_auth(
        ctx,
        &ResourceAccessRequest {
            resource: Resource::QueryAssetCatalogueItem,
            store_id: Some(store_id.clone()),
        },
    )?;
    let connection_manager = ctx.get_connection_manager().connection()?;
    let categories = get_asset_categories(
        &connection_manager,
        page.map(PaginationOption::from),
        filter.map(|filter| filter.to_domain()),
        sort.and_then(|mut sort_list| sort_list.pop())
            .map(|sort| sort.to_domain()),
    )
    .map_err(StandardGraphqlError::from_list_error)?;

    Ok(AssetCategoriesResponse::Response(
        AssetCategoryConnector::from_domain(categories),
    ))
}

pub fn asset_category(ctx: &Context<'_>, store_id: String, id:String) -> Result<AssetCategoryResponse> {
    validate_auth(
        ctx,
        &ResourceAccessRequest {
            resource: Resource::QueryAssetCatalogueItem,
            store_id: Some(store_id.clone()),
        },
    )?;
    let connection_manager = ctx.get_connection_manager().connection()?;
    let category = get_asset_category(&connection_manager, id)?;

    let response = match category {
        Some(category) => {
            AssetCategoryResponse::Response(AssetCategoryNode::from_domain(category))
        }
        None => AssetCategoryResponse::Error(NodeError {
            error: NodeErrorInterface::record_not_found(),
        })
    }
    Ok(response)
}

impl AssetCategoryFilterInput {
    pub fn to_domain(self) -> AssetCategoryFilter {
        let AssetCategoryFilter {
            id,
            name,
            class_id,
        } = self;
    }

    AssetCategoryFilter {
        id: id.map(EqualFilter::from),
        name: name.map(StringFilter::from),
        class_id: class_id.map(EqualFilter::from),
    }
}

impl AssetCategorySortInput {
    pub fn to_domain(self) -> AssetCategorySort {
        use AssetCateogyrSortField as to;
        use AssetCategorySortFieldInput as from;
        let key = match self.key {
            from::Name => to::Name,
        };

        AssetCategorySort {
            key,
            desc: self.desc,
        }
    }
}