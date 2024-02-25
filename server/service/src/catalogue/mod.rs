use self::query_catalogue_item::{get_asset_catalogue_item, get_asset_catalogue_items};
use self::query_category::{get_asset_categories, get_asset_category};
use self::query_class::{get_asset_class, get_asset_classes};

use super::{ListError, ListResult};
use crate::{service_provider::ServiceContext, SingleRecordError};
use repository::RepositoryError;
use repository::{
    assets::{
        asset_catalogue_item::{
            AssetCatalogueItem, AssetCatalogueItemFilter, AssetCatalogueItemSort,
        },
        asset_category::{AssetCategory, AssetCategoryFilter, AssetCategorySort},
        asset_class::{AssetClass, AssetClassFilter, AssetClassSort},
    },
    PaginationOption, StorageConnection,
};

pub mod query_catalogue_item;
pub mod query_category;
pub mod query_class;
pub mod query_type;
pub trait AssetCatalogueServiceTrait: Sync + Send {
    fn get_asset_catalogue_items(
        &self,
        connection: &StorageConnection,
        pagination: Option<PaginationOption>,
        filter: Option<AssetCatalogueItemFilter>,
        sort: Option<AssetCatalogueItemSort>,
    ) -> Result<ListResult<AssetCatalogueItem>, ListError> {
        get_asset_catalogue_items(connection, pagination, filter, sort)
    }

    fn get_asset_catalogue_item(
        &self,
        connection: &StorageConnection,
        id: String,
    ) -> Result<Option<AssetCatalogueItem>, RepositoryError> {
        get_asset_catalogue_item(connection, id)
    }

    fn get_asset_classes(
        &self,
        connection: &StorageConnection,
        pagination: Option<PaginationOption>,
        filter: Option<AssetClassFilter>,
        sort: Option<AssetClassSort>,
    ) -> Result<ListResult<AssetClass>, ListError> {
        get_asset_classes(connection, pagination, filter, sort)
    }

    fn get_asset_class(
        &self,
        ctx: &ServiceContext,
        id: String,
    ) -> Result<AssetClass, SingleRecordError> {
        get_asset_class(ctx, id)
    }

    fn get_asset_categories(
        &self,
        connection: &StorageConnection,
        pagination: Option<PaginationOption>,
        filter: Option<AssetCategoryFilter>,
        sort: Option<AssetCategorySort>,
    ) -> Result<ListResult<AssetCategory>, ListError> {
        get_asset_categories(connection, pagination, filter, sort)
    }

    fn get_asset_category(
        &self,
        ctx: &ServiceContext,
        id: String,
    ) -> Result<AssetCategory, SingleRecordError> {
        get_asset_category(ctx, id)
    }
}
