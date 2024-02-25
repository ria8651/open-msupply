use repository::{
    assets::asset_category::{
        AssetCategory, AssetCategoryFilter, AssetCategoryRepository, AssetCategorySort,
    },
    EqualFilter, PaginationOption, StorageConnection,
};

use crate::{get_default_pagination, i64_to_u32, ListError, ListResult, SingleRecordError};

pub const MAX_LIMIT: u32 = 1000;
pub const MIN_LIMIT: u32 = 1;

pub fn get_asset_categories(
    connection: &StorageConnection,
    pagination: Option<PaginationOption>,
    filter: Option<AssetCategoryFilter>,
    sort: Option<AssetCategorySort>,
) -> Result<ListResult<AssetCategory>, ListError> {
    let pagination = get_default_pagination(pagination, MAX_LIMIT, MIN_LIMIT)?;
    let repository = AssetCategoryRepository::new(&connection);
    Ok(ListResult {
        rows: repository.query(pagination, filter.clone(), sort)?,
        count: i64_to_u32(repository.count(filter)?),
    })
}

pub fn get_asset_category(
    connection: &StorageConnection,
    id: String,
) -> Result<Option<AssetCategory>, SingleRecordError> {
    let repository = AssetCategoryRepository::new(&connection);
    let mut result =
        repository.query_by_filter(AssetCategoryFilter::new().id(EqualFilter::equal_to(&id)))?;
    Ok(result.pop())
}
