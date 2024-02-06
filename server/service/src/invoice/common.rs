use repository::{
    EqualFilter, InvoiceLine, InvoiceLineFilter, InvoiceLineRepository, InvoiceLineRowType,
    InvoiceRow, MasterList, MasterListFilter, MasterListRepository, RepositoryError, StockLineRow,
    StorageConnection,
};
use util::inline_edit;

use crate::store_preference::get_store_preferences;

pub fn generate_invoice_user_id_update(
    user_id: &str,
    existing_invoice_row: InvoiceRow,
) -> Option<InvoiceRow> {
    let user_id_option = Some(user_id.to_string());
    let user_id_has_changed = existing_invoice_row.user_id != user_id_option;
    user_id_has_changed.then(|| {
        inline_edit(&existing_invoice_row, |mut u| {
            u.user_id = user_id_option;
            u
        })
    })
}

pub(crate) fn get_lines_for_invoice(
    connection: &StorageConnection,
    invoice_id: &str,
) -> Result<Vec<InvoiceLine>, RepositoryError> {
    let result = InvoiceLineRepository::new(connection)
        .query_by_filter(InvoiceLineFilter::new().invoice_id(EqualFilter::equal_to(invoice_id)))?;

    Ok(result)
}

pub fn calculate_total_after_tax(total_before_tax: f64, tax: Option<f64>) -> f64 {
    match tax {
        Some(tax) => total_before_tax * (1.0 + tax / 100.0),
        None => total_before_tax,
    }
}

pub fn calculate_foreign_currency_total(total: f64, currency_rate: Option<f64>) -> Option<f64> {
    match currency_rate {
        Some(currency_rate) => Some(total / currency_rate),
        None => None,
    }
}

#[derive(Debug, PartialEq)]
pub struct AddToShipmentFromMasterListInput {
    pub shipment_id: String,
    pub master_list_id: String,
}

pub fn check_master_list_for_name(
    connection: &StorageConnection,
    name_id: &str,
    master_list_id: &str,
) -> Result<Option<MasterList>, RepositoryError> {
    let mut rows = MasterListRepository::new(connection).query_by_filter(
        MasterListFilter::new()
            .id(EqualFilter::equal_to(master_list_id))
            .exists_for_name_id(EqualFilter::equal_to(name_id)),
    )?;
    Ok(rows.pop())
}

pub fn check_master_list_for_store(
    connection: &StorageConnection,
    store_id: &str,
    master_list_id: &str,
) -> Result<Option<MasterList>, RepositoryError> {
    let mut rows = MasterListRepository::new(connection).query_by_filter(
        MasterListFilter::new()
            .id(EqualFilter::equal_to(master_list_id))
            .exists_for_store_id(EqualFilter::equal_to(store_id)),
    )?;
    Ok(rows.pop())
}

pub fn check_can_issue_in_foreign_currency(
    connection: &StorageConnection,
    store_id: &str,
) -> Result<bool, RepositoryError> {
    let store_preferences = get_store_preferences(connection, store_id)?;
    Ok(store_preferences.issue_in_foreign_currency)
}

pub enum InvoiceLineHasNoStockLine {
    InvoiceLineHasNoStockLine(String),
    DatabaseError(RepositoryError),
}

// Returns a list of stock lines that need to be updated
pub fn generate_batches_total_number_of_packs_update(
    invoice_id: &str,
    connection: &StorageConnection,
) -> Result<Vec<StockLineRow>, InvoiceLineHasNoStockLine> {
    let invoice_lines = InvoiceLineRepository::new(connection)
        .query_by_filter(
            InvoiceLineFilter::new()
                .invoice_id(EqualFilter::equal_to(invoice_id))
                .r#type(InvoiceLineRowType::StockOut.equal_to()),
        )
        .map_err(|err| InvoiceLineHasNoStockLine::DatabaseError(err))?;

    let mut result = Vec::new();
    for invoice_line in invoice_lines {
        let invoice_line_row = invoice_line.invoice_line_row;
        let mut stock_line = invoice_line.stock_line_option.ok_or(
            InvoiceLineHasNoStockLine::InvoiceLineHasNoStockLine(invoice_line_row.id.to_owned()),
        )?;

        stock_line.total_number_of_packs =
            stock_line.total_number_of_packs - invoice_line_row.number_of_packs;
        result.push(stock_line);
    }
    Ok(result)
}
