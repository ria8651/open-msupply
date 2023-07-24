use repository::{
    Document, DocumentRegistryFilter, DocumentRegistryRepository, DocumentRepository, EqualFilter,
    RepositoryError, StorageConnection,
};

use crate::programs::{
    encounter::{
        encounter_updated::update_encounter_row, validate_misc::validate_encounter_schema,
    },
    patient::{patient_schema::SchemaPatient, patient_updated::update_patient_row},
    program_enrolment::program_enrolment_updated::update_program_enrolment_row,
    program_enrolment::program_schema::SchemaProgramEnrolment,
};

pub(crate) fn sync_upsert_document(
    con: &StorageConnection,
    document: &Document,
) -> Result<(), RepositoryError> {
    // TODO comment why only insert here
    DocumentRepository::new(con).sync_insert(document)?;

    let Some(registry) = DocumentRegistryRepository::new(con)
        .query_by_filter(
            DocumentRegistryFilter::new().document_type(EqualFilter::equal_to(&document.r#type)),
        )?
        .pop() else {
        log::warn!("Received unknown document type: {}", document.r#type);
        return Ok(());
    };

    match registry.r#type {
        repository::DocumentRegistryType::Patient => update_patient(con, document)?,
        repository::DocumentRegistryType::ProgramEnrolment => {
            update_program_enrolment(con, document)?
        }
        repository::DocumentRegistryType::Encounter => update_encounter(con, document)?,
        repository::DocumentRegistryType::Custom => {}
    };
    Ok(())
}

fn update_patient(con: &StorageConnection, document: &Document) -> Result<(), RepositoryError> {
    let patient: SchemaPatient = serde_json::from_value(document.data.clone()).map_err(|err| {
        RepositoryError::as_db_error(&format!("Invalid patient data: {}", err), "")
    })?;

    update_patient_row(con, &document.datetime, patient, true)
        .map_err(|err| RepositoryError::as_db_error(&format!("{:?}", err), ""))?;
    Ok(())
}

fn update_program_enrolment(
    con: &StorageConnection,
    document: &Document,
) -> Result<(), RepositoryError> {
    let Some(patient_id) = &document.owner_name_id else {
        return Err(RepositoryError::as_db_error("Document owner id expected", ""));
    };
    let program_enrolment: SchemaProgramEnrolment = serde_json::from_value(document.data.clone())
        .map_err(|err| {
        RepositoryError::as_db_error(&format!("Invalid program enrolment data: {}", err), "")
    })?;
    update_program_enrolment_row(con, patient_id, document, program_enrolment)
        .map_err(|err| RepositoryError::as_db_error(&format!("{:?}", err), ""))?;
    Ok(())
}

fn update_encounter(con: &StorageConnection, document: &Document) -> Result<(), RepositoryError> {
    let Some(patient_id) = &document.owner_name_id else {
        return Err(RepositoryError::as_db_error("Document owner id expected", ""));
    };

    let encounter: crate::programs::encounter::validate_misc::ValidatedSchemaEncounter =
        validate_encounter_schema(&document.data).map_err(|err| {
            RepositoryError::as_db_error(&format!("Invalid encounter data: {}", err), "")
        })?;

    let clinician_id = encounter
        .encounter
        .clinician
        .as_ref()
        .and_then(|c| c.id.clone());
    update_encounter_row(
        con,
        &patient_id,
        &document.context,
        document,
        encounter,
        clinician_id,
    )
    .map_err(|err| RepositoryError::as_db_error(&format!("{:?}", err), ""))?;
    Ok(())
}