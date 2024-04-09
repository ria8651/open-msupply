use repository::{
    ActivityLogType, Invoice, InvoiceLineRowRepository, InvoiceRow, InvoiceRowRepository,
    InvoiceRowStatus, StockLine, StockLineRowRepository,
};
use repository::{RepositoryError, StockLineRow};

use super::generate::generate;
use super::validate::validate;

use crate::activity_log::activity_log_entry;
use crate::invoice::query::get_invoice;
use crate::service_provider::ServiceContext;

#[derive(Clone, Debug, PartialEq)]

pub enum AdjustmentType {
    Addition,
    Reduction,
}

impl Default for AdjustmentType {
    fn default() -> Self {
        Self::Addition
    }
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct InsertInventoryAdjustment {
    pub stock_line_id: String,
    pub adjustment: f64,
    pub adjustment_type: AdjustmentType,
    pub inventory_adjustment_reason_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum InsertInventoryAdjustmentError {
    InvalidStore,
    StockLineDoesNotExist,
    StockLineReducedBelowZero(StockLineRow),
    InvalidAdjustment,
    AdjustmentReasonNotValid,
    AdjustmentReasonNotProvided,
    NewlyCreatedInvoiceDoesNotExist,
    DatabaseError(RepositoryError),
    InternalError(String),
}

pub fn insert_inventory_adjustment(
    ctx: &ServiceContext,
    input: InsertInventoryAdjustment,
) -> Result<Invoice, InsertInventoryAdjustmentError> {
    let invoice = ctx
        .connection
        .transaction_sync(|connection| {
            let stock_line = validate(connection, &ctx.store_id, &input)?;
            let (new_invoice, invoice_line, stock_line_row) =
                generate(connection, &ctx.store_id, &ctx.user_id, input, stock_line)?;

            let invoice_row_repo = InvoiceRowRepository::new(connection);

            invoice_row_repo.upsert_one(&new_invoice)?;
            InvoiceLineRowRepository::new(connection).upsert_one(&invoice_line)?;
            StockLineRowRepository::new(connection).upsert_one(&stock_line_row)?;

            let verified_invoice = InvoiceRow {
                status: InvoiceRowStatus::Verified,
                ..new_invoice
            };

            invoice_row_repo.upsert_one(&verified_invoice)?;

            activity_log_entry(
                ctx,
                ActivityLogType::InventoryAdjustment,
                Some(verified_invoice.id.to_owned()),
                None,
                None,
            )?;

            get_invoice(ctx, None, &verified_invoice.id)
                .map_err(InsertInventoryAdjustmentError::DatabaseError)?
                .ok_or(InsertInventoryAdjustmentError::NewlyCreatedInvoiceDoesNotExist)
        })
        .map_err(|error| error.to_inner_error())?;

    Ok(invoice)
}

impl From<RepositoryError> for InsertInventoryAdjustmentError {
    fn from(error: RepositoryError) -> Self {
        InsertInventoryAdjustmentError::DatabaseError(error)
    }
}

#[cfg(test)]
mod test {
    use repository::{
        mock::{
            mock_name_linked_to_store_join, mock_name_not_linked_to_store, mock_stock_line_a,
            mock_store_a, mock_store_b, mock_store_linked_to_name, mock_user_account_a, MockData,
            MockDataInserts,
        },
        test_db::{setup_all, setup_all_with_data},
        InvoiceRowRepository, InvoiceRowStatus, NameRow, NameStoreJoinRow,
    };
    use util::{inline_edit, inline_init};

    use crate::{
        invoice::inventory_adjustment::insert::InsertInventoryAdjustment,
        service_provider::ServiceProvider,
    };

    use super::InsertInventoryAdjustmentError;

    type ServiceError = InsertInventoryAdjustmentError;

    #[actix_rt::test]
    async fn insert_inventory_adjustment_errors() {
        let (_, _, connection_manager, _) =
            setup_all("insert_inventory_adjustment_errors", MockDataInserts::all()).await;

        let service_provider = ServiceProvider::new(connection_manager, "app_data");
        let mut context = service_provider
            .context(mock_store_a().id, "".to_string())
            .unwrap();
        let service = service_provider.invoice_service;

        // Stockline doesn't exist
        assert_eq!(
            service.insert_inventory_adjustment(
                &context,
                InsertInventoryAdjustment {
                    stock_line_id: "x".to_string(),
                    ..Default::default()
                }
            ),
            Err(ServiceError::StockLineDoesNotExist)
        );

        // Wrong store
        context.store_id = mock_store_b().id;
        assert_eq!(
            service.insert_inventory_adjustment(
                &context,
                InsertInventoryAdjustment {
                    stock_line_id: mock_stock_line_a().id,
                    ..Default::default()
                }
            ),
            Err(ServiceError::InvalidStore)
        );
        context.store_id = mock_store_a().id;

        // Missing reason
        // assert_eq!(
        //     service.insert_inventory_adjustment(
        //         &context,
        //         InsertInventoryAdjustment {
        //             stock_line_id: mock_stock_line_a().id,
        //             ..Default::default()
        //         }
        //     ),
        //     Err(ServiceError::AdjustmentReasonNotProvided)
        // );

        // Invalid reason
        assert_eq!(
            service.insert_inventory_adjustment(
                &context,
                InsertInventoryAdjustment {
                    stock_line_id: mock_stock_line_a().id,
                    inventory_adjustment_reason_id: Some("invalid".to_string()),
                    ..Default::default()
                }
            ),
            Err(ServiceError::InvalidAdjustment)
        );

        // Reduce stock below zero
        assert_eq!(
            service.insert_inventory_adjustment(
                &context,
                InsertInventoryAdjustment {
                    stock_line_id: mock_stock_line_a().id,
                    adjustment: 0.0,
                    ..Default::default()
                }
            ),
            Err(ServiceError::InvalidAdjustment)
        );

        // Reduce stock below zero
        assert_eq!(
            service.insert_inventory_adjustment(
                &context,
                InsertInventoryAdjustment {
                    stock_line_id: mock_stock_line_a().id,
                    adjustment_type:
                        crate::invoice::inventory_adjustment::AdjustmentType::Reduction,
                    adjustment: 50.0,
                    ..Default::default()
                }
            ),
            Err(ServiceError::StockLineReducedBelowZero(mock_stock_line_a()))
        );
    }

    #[actix_rt::test]
    async fn insert_inventory_adjustment_success() {
        let (_, connection, connection_manager, _) =
            setup_all("insert_inventory_adjustment_errors", MockDataInserts::all()).await;

        let service_provider = ServiceProvider::new(connection_manager, "app_data");
        let context = service_provider
            .context(mock_store_a().id, mock_user_account_a().id)
            .unwrap();
        let service = service_provider.invoice_service;

        // Success
        let created_invoice = service
            .insert_inventory_adjustment(
                &context,
                InsertInventoryAdjustment {
                    stock_line_id: mock_stock_line_a().id,
                    adjustment: 2.0,
                    ..Default::default()
                },
            )
            .unwrap();

        let retrieved_invoice = InvoiceRowRepository::new(&connection)
            .find_one_by_id(&created_invoice.invoice_row.id)
            .unwrap();

        assert_eq!(
            retrieved_invoice,
            inline_edit(&retrieved_invoice, |mut u| {
                u.id = created_invoice.invoice_row.id;
                u.status = InvoiceRowStatus::Verified;
                u
            })
        );
    }
}
