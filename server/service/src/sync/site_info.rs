use async_trait::async_trait;
use log::info;
use repository::{KeyValueStoreRepository, KeyValueType, RepositoryError};
use thiserror::Error;

use crate::{
    service_provider::{ServiceContext, ServiceProvider},
    sync::{
        api::{SiteInfoV5, SyncApiV5},
        settings::SyncSettings,
    },
};

use super::api::SyncApiError;

#[derive(Error, Debug)]
pub enum RequestAndSetSiteInfoError {
    #[error("Api error while requesting site info: {0:?}")]
    RequestSiteInfoError(SyncApiError),
    #[error("Database error whie requistin site info: {0:?}")]
    DatabaseError(RepositoryError),
    #[error("Unknown error while requesting and setting site info: {0:?}")]
    Other(#[from] anyhow::Error),
}

#[async_trait]
pub trait SiteInfoTrait: Sync + Send {
    async fn request_and_set_site_info(
        &self,
        service_provider: &ServiceProvider,
        settings: &SyncSettings,
    ) -> Result<SiteInfoV5, RequestAndSetSiteInfoError>;
    fn get_site_id(&self, ctx: &ServiceContext) -> Result<Option<i32>, RepositoryError>;
}

pub struct SiteInfoService;

#[async_trait]
impl SiteInfoTrait for SiteInfoService {
    async fn request_and_set_site_info(
        &self,
        service_provider: &ServiceProvider,
        settings: &SyncSettings,
    ) -> Result<SiteInfoV5, RequestAndSetSiteInfoError> {
        use RequestAndSetSiteInfoError as Error;

        let sync_api_v5 = SyncApiV5::new(&settings, &service_provider)?;
        let ctx = service_provider
            .basic_context()
            .map_err(Error::DatabaseError)?;

        info!("Requesting site info");
        let site_info = sync_api_v5
            .get_site_info()
            .await
            .map_err(Error::RequestSiteInfoError)?;

        let repo = KeyValueStoreRepository::new(&ctx.connection);
        repo.set_string(
            KeyValueType::SettingsSyncSiteUuid,
            Some(site_info.id.clone()),
        )
        .map_err(Error::DatabaseError)?;
        repo.set_i32(
            KeyValueType::SettingsSyncSiteId,
            Some(site_info.site_id.clone()),
        )
        .map_err(Error::DatabaseError)?;

        info!("Received site info");

        Ok(site_info)
    }

    fn get_site_id(&self, ctx: &ServiceContext) -> Result<Option<i32>, RepositoryError> {
        let site_id = KeyValueStoreRepository::new(&ctx.connection)
            .get_i32(KeyValueType::SettingsSyncSiteId)?;

        Ok(site_id)
    }
}