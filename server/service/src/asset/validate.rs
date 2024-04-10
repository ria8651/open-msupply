use repository::{
    asset_internal_location::{AssetInternalLocationFilter, AssetInternalLocationRepository},
    asset_internal_location_row::AssetInternalLocationRow,
    asset_log_reason_row::AssetLogReasonRowRepository,
    asset_log_row::AssetLogStatus,
    assets::{
        asset_log_row::{AssetLogRow, AssetLogRowRepository},
        asset_row::{AssetRow, AssetRowRepository},
    },
    EqualFilter, RepositoryError, StorageConnection,
};

pub fn check_asset_exists(
    id: &str,
    connection: &StorageConnection,
) -> Result<Option<AssetRow>, RepositoryError> {
    AssetRowRepository::new(connection).find_one_by_id(id)
}

pub fn check_asset_log_exists(
    id: &str,
    connection: &StorageConnection,
) -> Result<Option<AssetLogRow>, RepositoryError> {
    AssetLogRowRepository::new(connection).find_one_by_id(id)
}

pub fn check_reason_matches_status(
    status: &Option<AssetLogStatus>,
    reason_id: &Option<String>,
    connection: &StorageConnection,
) -> bool {
    if let Some(reason_id) = reason_id {
        match status {
            Some(status) => {
                let reason = AssetLogReasonRowRepository::new(connection).find_one_by_id(reason_id);
                if let Ok(Some(reason)) = reason {
                    return reason.asset_log_status == *status;
                } else {
                    return false;
                }
            }
            None => return false,
        }
    }

    // TODO - add function to check reason matches status
    true
}

pub fn check_locations_are_assigned(
    location_ids: Vec<String>,
    asset_id: &str,
    connection: &StorageConnection,
) -> Result<Vec<AssetInternalLocationRow>, RepositoryError> {
    Ok(
        AssetInternalLocationRepository::new(connection).query_by_filter(
            AssetInternalLocationFilter::new()
                .location_id(EqualFilter::equal_any(location_ids))
                .asset_id(EqualFilter::not_equal_to(asset_id)),
        )?,
    )
}
