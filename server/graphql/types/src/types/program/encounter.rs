use async_graphql::{dataloader::DataLoader, *};
use chrono::{DateTime, Utc};
use graphql_core::{
    generic_filters::{DatetimeFilterInput, EqualFilterStringInput, StringFilterInput},
    loader::{
        ClinicianLoader, ClinicianLoaderInput, DocumentLoader, NameByIdLoader, NameByIdLoaderInput,
        ProgramEnrolmentLoader, ProgramEnrolmentLoaderInput,
    },
    map_filter,
    standard_graphql_error::StandardGraphqlError,
    ContextExt,
};
use repository::{
    DatetimeFilter, EncounterFilter, EncounterRow, EncounterSort, EncounterSortField,
    EncounterStatus, EqualFilter, ProgramEventFilter, ProgramEventSortField, Sort, StringFilter,
};
use serde::Serialize;

use crate::types::{ClinicianNode, NameNode};

use super::{
    document::DocumentNode, program_enrolment::ProgramEnrolmentNode,
    program_event::ProgramEventNode,
};

pub struct EncounterNode {
    pub store_id: String,
    pub encounter_row: EncounterRow,
    pub allowed_ctx: Vec<String>,
}

#[derive(SimpleObject)]
pub struct EncounterConnector {
    pub total_count: u32,
    pub nodes: Vec<EncounterNode>,
}

#[derive(InputObject, Clone)]
pub struct EqualFilterEncounterStatusInput {
    pub equal_to: Option<EncounterNodeStatus>,
    pub equal_any: Option<Vec<EncounterNodeStatus>>,
    pub not_equal_to: Option<EncounterNodeStatus>,
}

#[derive(Enum, Copy, Clone, PartialEq, Eq)]
#[graphql(rename_items = "camelCase")]
pub enum EncounterSortFieldInput {
    Type,
    PatientId,
    Program,
    CreatedDatetime,
    StartDatetime,
    EndDatetime,
    Status,
}

#[derive(InputObject)]
pub struct EncounterSortInput {
    /// Sort query result by `key`
    key: EncounterSortFieldInput,
    /// Sort query result is sorted descending or ascending (if not provided the default is
    /// ascending)
    desc: Option<bool>,
}

impl EncounterSortInput {
    pub fn to_domain(self) -> EncounterSort {
        let key = match self.key {
            EncounterSortFieldInput::Type => EncounterSortField::DocumentType,
            EncounterSortFieldInput::PatientId => EncounterSortField::PatientId,
            EncounterSortFieldInput::Program => EncounterSortField::Context,
            EncounterSortFieldInput::CreatedDatetime => EncounterSortField::CreatedDatetime,
            EncounterSortFieldInput::StartDatetime => EncounterSortField::StartDatetime,
            EncounterSortFieldInput::EndDatetime => EncounterSortField::EndDatetime,
            EncounterSortFieldInput::Status => EncounterSortField::Status,
        };

        EncounterSort {
            key,
            desc: self.desc,
        }
    }
}

#[derive(InputObject, Clone)]
pub struct EncounterFilterInput {
    pub id: Option<EqualFilterStringInput>,
    pub r#type: Option<EqualFilterStringInput>,
    pub patient_id: Option<EqualFilterStringInput>,
    /// The program name
    pub program: Option<EqualFilterStringInput>,
    pub created_datetime: Option<DatetimeFilterInput>,
    pub start_datetime: Option<DatetimeFilterInput>,
    pub end_datetime: Option<DatetimeFilterInput>,
    pub status: Option<EqualFilterEncounterStatusInput>,
    pub clinician_id: Option<EqualFilterStringInput>,
    pub document_name: Option<EqualFilterStringInput>,
    pub document_data: Option<StringFilterInput>,
}

impl EncounterFilterInput {
    pub fn to_domain_filter(self) -> EncounterFilter {
        EncounterFilter {
            id: self.id.map(EqualFilter::from),
            patient_id: self.patient_id.map(EqualFilter::from),
            context: self.program.map(EqualFilter::from),
            created_datetime: self.created_datetime.map(DatetimeFilter::from),
            start_datetime: self.start_datetime.map(DatetimeFilter::from),
            status: self
                .status
                .map(|s| map_filter!(s, EncounterNodeStatus::to_domain)),
            end_datetime: self.end_datetime.map(DatetimeFilter::from),
            clinician_id: self.clinician_id.map(EqualFilter::from),
            document_type: self.r#type.map(EqualFilter::from),
            document_name: self.document_name.map(EqualFilter::from),
            document_data: self.document_data.map(StringFilter::from),
        }
    }
}

#[derive(Enum, Copy, Clone, PartialEq, Eq, Debug, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")] // only needed to be comparable in tests
pub enum EncounterNodeStatus {
    Pending,
    Visited,
    Cancelled,
}

impl EncounterNodeStatus {
    pub fn to_domain(self) -> EncounterStatus {
        match self {
            EncounterNodeStatus::Pending => EncounterStatus::Pending,
            EncounterNodeStatus::Visited => EncounterStatus::Visited,
            EncounterNodeStatus::Cancelled => EncounterStatus::Cancelled,
        }
    }

    pub fn from_domain(status: &EncounterStatus) -> EncounterNodeStatus {
        match status {
            EncounterStatus::Pending => EncounterNodeStatus::Pending,
            EncounterStatus::Visited => EncounterNodeStatus::Visited,
            EncounterStatus::Cancelled => EncounterNodeStatus::Cancelled,
        }
    }
}

#[derive(InputObject, Clone)]
pub struct EncounterEventFilterInput {
    pub r#type: Option<EqualFilterStringInput>,
    /// Only include events that are for the current encounter, i.e. have matching encounter type
    /// and matching encounter name of the current encounter. If not set all events with matching
    /// encounter type are returned.
    pub is_current_encounter: Option<bool>,
}

impl EncounterEventFilterInput {
    pub fn to_domain(&self) -> ProgramEventFilter {
        ProgramEventFilter {
            datetime: None,
            active_start_datetime: None,
            active_end_datetime: None,
            patient_id: None,
            document_type: None,
            document_name: None,
            r#type: self.r#type.clone().map(EqualFilter::from),
            context: None,
        }
    }
}

#[Object]
impl EncounterNode {
    pub async fn id(&self) -> &str {
        &self.encounter_row.id
    }

    pub async fn context(&self) -> &str {
        &self.encounter_row.context
    }

    pub async fn patient_id(&self) -> &str {
        &self.encounter_row.patient_id
    }

    pub async fn patient(&self, ctx: &Context<'_>) -> Result<NameNode> {
        let loader = ctx.get_loader::<DataLoader<NameByIdLoader>>();

        let result = loader
            .load_one(NameByIdLoaderInput::new(
                &self.store_id,
                &self.encounter_row.patient_id,
            ))
            .await?
            .map(NameNode::from_domain)
            .ok_or(Error::new("Encounter without patient"))?;

        Ok(result)
    }

    pub async fn clinician(&self, ctx: &Context<'_>) -> Result<Option<ClinicianNode>> {
        let Some(clinician_id) = self.encounter_row.clinician_id.as_ref() else {
            return Ok(None)
        };
        let loader = ctx.get_loader::<DataLoader<ClinicianLoader>>();

        let result = loader
            .load_one(ClinicianLoaderInput::new(&self.store_id, &clinician_id))
            .await?
            .map(ClinicianNode::from_domain)
            .ok_or(Error::new(format!(
                "Failed to load clinician: {}",
                clinician_id
            )))?;

        Ok(Some(result))
    }

    /// Returns the matching program enrolment for the patient of this encounter
    pub async fn program_enrolment(
        &self,
        ctx: &Context<'_>,
    ) -> Result<Option<ProgramEnrolmentNode>> {
        let loader = ctx.get_loader::<DataLoader<ProgramEnrolmentLoader>>();

        let result = loader
            .load_one(ProgramEnrolmentLoaderInput::new(
                &self.encounter_row.patient_id,
                &self.encounter_row.context,
                self.allowed_ctx.clone(),
            ))
            .await?
            .map(|program_row| ProgramEnrolmentNode {
                store_id: self.store_id.clone(),
                program_row,
                allowed_ctx: self.allowed_ctx.clone(),
            })
            .ok_or(Error::new(format!(
                "Failed to load program enrolment: {}",
                self.encounter_row.context
            )))?;

        Ok(Some(result))
    }

    pub async fn r#type(&self) -> &str {
        &self.encounter_row.document_type
    }

    pub async fn name(&self) -> &str {
        &self.encounter_row.document_name
    }

    pub async fn status(&self) -> Option<EncounterNodeStatus> {
        self.encounter_row
            .status
            .as_ref()
            .map(|status| EncounterNodeStatus::from_domain(status))
    }

    pub async fn created_datetime(&self) -> DateTime<Utc> {
        DateTime::<Utc>::from_utc(self.encounter_row.created_datetime, Utc)
    }

    pub async fn start_datetime(&self) -> DateTime<Utc> {
        DateTime::<Utc>::from_utc(self.encounter_row.start_datetime, Utc)
    }

    pub async fn end_datetime(&self) -> Option<DateTime<Utc>> {
        self.encounter_row
            .end_datetime
            .map(|t| DateTime::<Utc>::from_utc(t, Utc))
    }

    /// The encounter document
    pub async fn document(&self, ctx: &Context<'_>) -> Result<DocumentNode> {
        let loader = ctx.get_loader::<DataLoader<DocumentLoader>>();

        let result = loader
            .load_one(self.encounter_row.document_name.clone())
            .await?
            .map(|document| DocumentNode {
                allowed_ctx: self.allowed_ctx.clone(),
                document,
            })
            .ok_or(Error::new("Program without document"))?;

        Ok(result)
    }

    pub async fn active_program_events(
        &self,
        ctx: &Context<'_>,
        at: Option<DateTime<Utc>>,
        filter: Option<EncounterEventFilterInput>,
    ) -> Result<Vec<ProgramEventNode>> {
        // TODO use loader?
        let context = ctx.service_provider().basic_context()?;
        let mut program_filter = filter
            .as_ref()
            .map(|f| f.to_domain())
            .unwrap_or(ProgramEventFilter::new())
            .patient_id(EqualFilter::equal_to(&self.encounter_row.patient_id))
            .document_type(EqualFilter::equal_to(&self.encounter_row.document_type));
        if filter.and_then(|f| f.is_current_encounter).unwrap_or(false) {
            program_filter = program_filter
                .document_name(EqualFilter::equal_to(&self.encounter_row.document_name))
        };
        let entries = ctx
            .service_provider()
            .program_event_service
            .active_events(
                &context,
                at.map(|at| at.naive_utc())
                    .unwrap_or(Utc::now().naive_utc()),
                None,
                Some(program_filter),
                Some(Sort {
                    key: ProgramEventSortField::Datetime,
                    desc: Some(true),
                }),
            )
            .map_err(StandardGraphqlError::from_list_error)?;
        Ok(entries
            .rows
            .into_iter()
            .map(|row| ProgramEventNode {
                store_id: self.store_id.clone(),
                row,
                allowed_ctx: self.allowed_ctx.clone(),
            })
            .collect())
    }
}