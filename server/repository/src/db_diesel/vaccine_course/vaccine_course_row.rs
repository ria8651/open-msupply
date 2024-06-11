use super::vaccine_course_row::vaccine_course::dsl::*;

use serde::Deserialize;
use serde::Serialize;

use crate::RepositoryError;
use crate::StorageConnection;

use diesel::prelude::*;

table! {
    vaccine_course (id) {
        id -> Text,
        name -> Text,
        program_id -> Text,
        demographic_indicator_id -> Text,
        coverage_rate -> Double,
        is_active -> Bool,
        wastage_rate -> Double,
        doses -> Integer,
    }
}

#[derive(
    Clone, Insertable, Queryable, Debug, PartialEq, AsChangeset, Serialize, Deserialize, Default,
)]
#[diesel(table_name = vaccine_course)]
pub struct VaccineCourseRow {
    pub id: String,
    pub name: String,
    pub program_id: String,
    pub demographic_indicator_id: String,
    pub coverage_rate: f64,
    pub is_active: bool,
    pub wastage_rate: f64,
    pub doses: i32,
}

pub struct VaccineCourseRowRepository<'a> {
    connection: &'a StorageConnection,
}

impl<'a> VaccineCourseRowRepository<'a> {
    pub fn new(connection: &'a StorageConnection) -> Self {
        VaccineCourseRowRepository { connection }
    }

    #[cfg(feature = "postgres")]
    pub fn upsert_one(&self, vaccine_course_row: &VaccineCourseRow) -> Result<(), RepositoryError> {
        diesel::insert_into(vaccine_course)
            .values(vaccine_course_row)
            .on_conflict(id)
            .do_update()
            .set(vaccine_course_row)
            .execute(self.connection.lock().connection())?;
        Ok(())
    }

    #[cfg(not(feature = "postgres"))]
    pub fn upsert_one(&self, vaccine_course_row: &VaccineCourseRow) -> Result<(), RepositoryError> {
        diesel::replace_into(vaccine_course)
            .values(vaccine_course_row)
            .execute(self.connection.lock().connection())?;
        Ok(())
    }

    pub fn find_all(&mut self) -> Result<Vec<VaccineCourseRow>, RepositoryError> {
        let result = vaccine_course.load(self.connection.lock().connection());
        Ok(result?)
    }

    pub fn find_one_by_id(
        &self,
        vaccine_course_id: &str,
    ) -> Result<Option<VaccineCourseRow>, RepositoryError> {
        let result = vaccine_course
            .filter(id.eq(vaccine_course_id))
            .first(self.connection.lock().connection())
            .optional()?;
        Ok(result)
    }

    pub fn delete(&self, vaccine_course_id: &str) -> Result<(), RepositoryError> {
        diesel::delete(vaccine_course)
            .filter(id.eq(vaccine_course_id))
            .execute(self.connection.lock().connection())?;
        Ok(())
    }
}