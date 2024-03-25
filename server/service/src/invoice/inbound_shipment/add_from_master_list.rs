use crate::invoice::common::{
    get_lines_for_invoice, AddToShipmentFromMasterListInput as ServiceInput,
};
use crate::invoice::{check_invoice_exists, common::check_master_list_for_store};
use crate::service_provider::ServiceContext;
use repository::{EqualFilter, ItemRowType};
use repository::{
    InvoiceLine, InvoiceLineFilter, InvoiceLineRepository, InvoiceLineRow,
    InvoiceLineRowRepository, InvoiceRow, InvoiceRowStatus, InvoiceRowType, MasterListLineFilter,
    MasterListLineRepository, RepositoryError, StorageConnection,
};

use super::generate_empty_invoice_lines;

#[derive(Debug, PartialEq)]
pub enum AddToInboundShipmentFromMasterListError {
    ShipmentDoesNotExist,
    NotThisStoreShipment,
    CannotEditShipment,
    MasterListNotFoundForThisStore,
    NotAnInboundShipment,
    DatabaseError(RepositoryError),
}

type InError = AddToInboundShipmentFromMasterListError;

impl From<RepositoryError> for AddToInboundShipmentFromMasterListError {
    fn from(error: RepositoryError) -> Self {
        AddToInboundShipmentFromMasterListError::DatabaseError(error)
    }
}

pub fn add_from_master_list(
    ctx: &ServiceContext,
    input: ServiceInput,
) -> Result<Vec<InvoiceLine>, InError> {
    let invoice_lines = ctx
        .connection
        .transaction_sync(|connection| {
            let invoice_row = validate(connection, &ctx.store_id, &input)?;
            let new_invoice_line_rows = generate(ctx, invoice_row, &input)?;

            let invoice_line_row_repository = InvoiceLineRowRepository::new(&connection);

            for invoice_line_row in new_invoice_line_rows {
                invoice_line_row_repository.upsert_one(&invoice_line_row)?;
            }

            match InvoiceLineRepository::new(connection).query_by_filter(
                InvoiceLineFilter::new().invoice_id(EqualFilter::equal_to(&input.shipment_id)),
            ) {
                Ok(lines) => Ok(lines),
                Err(error) => Err(InError::DatabaseError(error)),
            }
        })
        .map_err(|error| error.to_inner_error())?;
    Ok(invoice_lines)
}

fn validate(
    connection: &StorageConnection,
    store_id: &str,
    input: &ServiceInput,
) -> Result<InvoiceRow, InError> {
    let invoice_row = check_invoice_exists(&input.shipment_id, connection)?
        .ok_or(InError::ShipmentDoesNotExist)?;

    if invoice_row.store_id != store_id {
        return Err(InError::NotThisStoreShipment);
    }
    if invoice_row.status != InvoiceRowStatus::New {
        return Err(InError::CannotEditShipment);
    }

    if invoice_row.r#type != InvoiceRowType::InboundShipment {
        return Err(InError::NotAnInboundShipment);
    }

    check_master_list_for_store(connection, &invoice_row.store_id, &input.master_list_id)?
        .ok_or(InError::MasterListNotFoundForThisStore)?;

    Ok(invoice_row)
}

fn generate(
    ctx: &ServiceContext,
    invoice_row: InvoiceRow,
    input: &ServiceInput,
) -> Result<Vec<InvoiceLineRow>, RepositoryError> {
    let invoice_lines = get_lines_for_invoice(&ctx.connection, &input.shipment_id)?;

    let item_ids_in_invoice: Vec<String> = invoice_lines
        .into_iter()
        .map(|invoice_line| invoice_line.item_row.id)
        .collect();

    let master_list_lines_not_in_invoice = MasterListLineRepository::new(&ctx.connection)
        .query_by_filter(
            MasterListLineFilter::new()
                .master_list_id(EqualFilter::equal_to(&input.master_list_id))
                .item_id(EqualFilter::not_equal_all(item_ids_in_invoice))
                .item_type(ItemRowType::Stock.equal_to()),
        )?;

    let items_ids_not_in_invoice: Vec<String> = master_list_lines_not_in_invoice
        .into_iter()
        .map(|master_list_line| master_list_line.item_id)
        .collect();

    Ok(generate_empty_invoice_lines(
        ctx,
        &invoice_row,
        items_ids_not_in_invoice,
    )?)
}

#[cfg(test)]
mod test {
    use crate::invoice::common::AddToShipmentFromMasterListInput as ServiceInput;
    use crate::invoice::inbound_shipment::AddToInboundShipmentFromMasterListError as ServiceError;
    use crate::service_provider::ServiceProvider;
    use repository::{
        mock::{
            common::FullMockMasterList, mock_empty_draft_inbound_shipment, mock_inbound_shipment_a,
            mock_inbound_shipment_c, mock_item_a, mock_item_b, mock_item_c, mock_item_d,
            mock_name_store_a, mock_outbound_shipment_c, mock_store_a, mock_store_c,
            mock_test_not_store_a_master_list, MockData, MockDataInserts,
        },
        test_db::{setup_all, setup_all_with_data},
        MasterListLineRow, MasterListNameJoinRow, MasterListRow,
    };
    use util::inline_init;

    #[actix_rt::test]
    async fn add_from_master_list_errors() {
        let (_, _, connection_manager, _) =
            setup_all("is_add_from_master_list_errors", MockDataInserts::all()).await;

        let service_provider = ServiceProvider::new(connection_manager, "app_data");
        let mut context = service_provider
            .context(mock_store_a().id, "".to_string())
            .unwrap();
        let service = service_provider.invoice_service;

        // RecordDoesNotExist
        assert_eq!(
            service.add_to_inbound_shipment_from_master_list(
                &context,
                ServiceInput {
                    shipment_id: "invalid".to_owned(),
                    master_list_id: "n/a".to_owned()
                },
            ),
            Err(ServiceError::ShipmentDoesNotExist)
        );

        // CannotEditRecord
        assert_eq!(
            service.add_to_inbound_shipment_from_master_list(
                &context,
                ServiceInput {
                    shipment_id: mock_inbound_shipment_a().id,
                    master_list_id: "n/a".to_owned()
                },
            ),
            Err(ServiceError::CannotEditShipment)
        );

        // MasterListNotFoundForThisStore
        assert_eq!(
            service.add_to_inbound_shipment_from_master_list(
                &context,
                ServiceInput {
                    shipment_id: mock_inbound_shipment_c().id,
                    master_list_id: mock_test_not_store_a_master_list().master_list.id
                },
            ),
            Err(ServiceError::MasterListNotFoundForThisStore)
        );

        // NotThisStore
        context.store_id = mock_store_c().id;
        assert_eq!(
            service.add_to_inbound_shipment_from_master_list(
                &context,
                ServiceInput {
                    shipment_id: mock_inbound_shipment_c().id,
                    master_list_id: "n/a".to_owned()
                },
            ),
            Err(ServiceError::NotThisStoreShipment)
        );

        // RecordIsIncorrectType
        assert_eq!(
            service.add_to_inbound_shipment_from_master_list(
                &context,
                ServiceInput {
                    shipment_id: mock_outbound_shipment_c().id,
                    master_list_id: "n/a".to_owned()
                },
            ),
            Err(ServiceError::NotAnInboundShipment)
        );
    }

    #[actix_rt::test]
    async fn add_from_master_list_success() {
        fn master_list() -> FullMockMasterList {
            let id = "master_list".to_owned();
            let join1 = format!("{}1", id);
            let line1 = format!("{}1", id);
            let line2 = format!("{}2", id);
            let line3 = format!("{}3", id);
            let line4 = format!("{}4", id);

            FullMockMasterList {
                master_list: MasterListRow {
                    id: id.clone(),
                    name: id.clone(),
                    code: id.clone(),
                    description: id.clone(),
                    is_active: true,
                },
                joins: vec![MasterListNameJoinRow {
                    id: join1,
                    master_list_id: id.clone(),
                    name_link_id: mock_name_store_a().id,
                }],
                lines: vec![
                    MasterListLineRow {
                        id: line1.clone(),
                        item_link_id: mock_item_a().id,
                        master_list_id: id.clone(),
                    },
                    MasterListLineRow {
                        id: line2.clone(),
                        item_link_id: mock_item_b().id,
                        master_list_id: id.clone(),
                    },
                    MasterListLineRow {
                        id: line3.clone(),
                        item_link_id: mock_item_c().id,
                        master_list_id: id.clone(),
                    },
                    MasterListLineRow {
                        id: line4.clone(),
                        item_link_id: mock_item_d().id,
                        master_list_id: id.clone(),
                    },
                ],
            }
        }

        let (_, _, connection_manager, _) = setup_all_with_data(
            "is_add_from_master_list_success",
            MockDataInserts::all(),
            inline_init(|r: &mut MockData| {
                r.full_master_lists = vec![master_list()];
            }),
        )
        .await;

        let service_provider = ServiceProvider::new(connection_manager, "app_data");
        let context = service_provider
            .context(mock_store_a().id, "".to_string())
            .unwrap();
        let service = service_provider.invoice_service;

        let result: Vec<repository::InvoiceLineRow> = service
            .add_to_inbound_shipment_from_master_list(
                &context,
                ServiceInput {
                    shipment_id: mock_empty_draft_inbound_shipment().id,
                    master_list_id: master_list().master_list.id,
                },
            )
            .unwrap()
            .into_iter()
            .map(|line| line.invoice_line_row)
            .collect();

        let mut item_ids: Vec<String> = result
            .clone()
            .into_iter()
            .map(|invoice_line| invoice_line.item_link_id)
            .collect();
        item_ids.sort_by(|a, b| a.cmp(&b));

        let mut test_item_ids = vec![
            mock_item_a().id,
            mock_item_b().id,
            mock_item_c().id,
            mock_item_d().id,
        ];
        test_item_ids.sort_by(|a, b| a.cmp(&b));

        assert_eq!(item_ids, test_item_ids);
        let line = result
            .iter()
            .find(|line| line.item_link_id == mock_item_a().id)
            .unwrap();

        assert_eq!(line.number_of_packs, 0.0);
        assert_eq!(line.item_name, mock_item_a().name);
        assert_eq!(line.item_code, mock_item_a().code);

        let line = result
            .iter()
            .find(|line| line.item_link_id == mock_item_b().id)
            .unwrap();

        assert_eq!(line.number_of_packs, 0.0);
        assert_eq!(line.item_name, mock_item_b().name);
        assert_eq!(line.item_code, mock_item_b().code);
    }
}
