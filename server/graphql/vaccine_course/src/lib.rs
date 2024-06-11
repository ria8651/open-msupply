use async_graphql::*;
use graphql_core::pagination::PaginationInput;
use mutations::{insert_vaccine_course, InsertVaccineCourseInput, InsertVaccineCourseResponse};
use types::vaccine_course::{VaccineCourseResponse, VaccineCoursesResponse};

pub mod vaccine_course_queries;
use crate::vaccine_course_queries::*;
pub mod mutations;
pub mod types;

#[derive(Default, Clone)]
pub struct VaccineCourseQueries;
#[Object]
impl VaccineCourseQueries {
    pub async fn vaccine_courses(
        &self,
        ctx: &Context<'_>,
        #[graphql(desc = "Pagination option (first and offset)")] page: Option<PaginationInput>,
        #[graphql(desc = "Filter option")] filter: Option<VaccineCourseFilterInput>,
        #[graphql(desc = "Sort options (only first sort input is evaluated for this endpoint)")]
        sort: Option<Vec<VaccineCourseSortInput>>,
    ) -> Result<VaccineCoursesResponse> {
        vaccine_courses(ctx, page, filter, sort)
    }

    pub async fn vaccine_course(
        &self,
        ctx: &Context<'_>,
        id: String,
    ) -> Result<VaccineCourseResponse> {
        vaccine_course(ctx, id)
    }
}

#[derive(Default, Clone)]
pub struct VaccineCourseMutations;

#[Object]
impl VaccineCourseMutations {
    async fn insert_vaccine_course(
        &self,
        ctx: &Context<'_>,
        store_id: String,
        input: InsertVaccineCourseInput,
    ) -> Result<InsertVaccineCourseResponse> {
        insert_vaccine_course(ctx, &store_id, input)
    }
}