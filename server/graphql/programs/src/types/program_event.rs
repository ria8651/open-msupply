use async_graphql::{dataloader::DataLoader, *};
use chrono::{DateTime, Utc};

use graphql_core::{
    loader::{DocumentLoader, PatientLoader},
    ContextExt,
};
use repository::ProgramEventRow;

use super::{document::DocumentNode, patient::PatientNode};

pub struct ProgramEventNode {
    pub store_id: String,
    pub row: ProgramEventRow,
    pub allowed_ctx: Vec<String>,
}

#[Object]
impl ProgramEventNode {
    pub async fn patient_id(&self) -> &Option<String> {
        &self.row.patient_id
    }

    pub async fn patient(&self, ctx: &Context<'_>) -> Result<Option<PatientNode>> {
        let Some(patient_id) = &self.row.patient_id else {
            return Ok(None);
        };
        let loader = ctx.get_loader::<DataLoader<PatientLoader>>();

        let result = loader
            .load_one(patient_id.clone())
            .await?
            .map(|patient| PatientNode {
                store_id: self.store_id.clone(),
                allowed_ctx: self.allowed_ctx.clone(),
                patient,
            })
            .ok_or(Error::new(format!(
                "Failed to load patient: {}",
                patient_id
            )))?;

        Ok(Some(result))
    }

    pub async fn datetime(&self) -> DateTime<Utc> {
        DateTime::<Utc>::from_utc(self.row.datetime, Utc)
    }

    pub async fn active_datetime(&self) -> DateTime<Utc> {
        DateTime::<Utc>::from_utc(self.row.active_start_datetime, Utc)
    }

    pub async fn document_type(&self) -> &str {
        &&self.row.document_type
    }

    pub async fn document_name(&self) -> &Option<String> {
        &self.row.document_name
    }

    pub async fn data(&self) -> &Option<String> {
        &self.row.data
    }

    pub async fn r#type(&self) -> &str {
        &self.row.r#type
    }

    /// The document associated with the document_name
    pub async fn document(&self, ctx: &Context<'_>) -> Result<Option<DocumentNode>> {
        let Some(document_name) = self.row.document_name.clone() else {
            return Ok(None);
        };
        let loader = ctx.get_loader::<DataLoader<DocumentLoader>>();

        let result = loader
            .load_one(document_name.clone())
            .await?
            .map(|document| DocumentNode {
                allowed_ctx: self.allowed_ctx.clone(),
                document,
            })
            .ok_or(Error::new(format!(
                "Failed to load document: {}",
                document_name
            )))?;

        Ok(Some(result))
    }
}