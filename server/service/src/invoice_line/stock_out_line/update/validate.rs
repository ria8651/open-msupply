use repository::{InvoiceLineRow, InvoiceRow, InvoiceStatus, ItemRow, StorageConnection};

use crate::{
    invoice::{check_invoice_exists, check_invoice_is_editable, check_invoice_type, check_store},
    invoice_line::{
        check_batch_exists, check_batch_on_hold, check_existing_stock_line, check_location_on_hold,
        stock_out_line::BatchPair,
        validate::{check_line_belongs_to_invoice, check_line_exists, check_number_of_packs},
        LocationIsOnHoldError,
    },
};

use super::{UpdateStockOutLine, UpdateStockOutLineError};

pub fn validate(
    input: &UpdateStockOutLine,
    store_id: &str,
    connection: &StorageConnection,
) -> Result<(InvoiceLineRow, ItemRow, BatchPair, InvoiceRow), UpdateStockOutLineError> {
    use UpdateStockOutLineError::*;

    let line = check_line_exists(connection, &input.id)?.ok_or(LineDoesNotExist)?;
    let line_row = &line.invoice_line_row;
    let invoice =
        check_invoice_exists(&line_row.invoice_id, connection)?.ok_or(InvoiceDoesNotExist)?;
    if !check_store(&invoice, store_id) {
        return Err(NotThisStoreInvoice);
    }
    let existing_stock = check_existing_stock_line(
        &line_row.id.clone(),
        &invoice.id,
        input.stock_line_id.clone(),
        connection,
    )?;
    if let Some(existing_stock) = existing_stock {
        return Err(StockLineAlreadyExistsInInvoice(existing_stock.id));
    }

    if let Some(r#type) = &input.r#type {
        if !check_invoice_type(&invoice, r#type.to_domain()) {
            return Err(InvoiceTypeDoesNotMatch);
        }
    } else {
        return Err(NoInvoiceType);
    }
    if !check_invoice_is_editable(&invoice) {
        return Err(CannotEditFinalised);
    }
    if !check_line_belongs_to_invoice(line_row, &invoice) {
        return Err(NotThisInvoiceLine(line.invoice_line_row.invoice_id));
    }
    if invoice.status != InvoiceStatus::New && !check_number_of_packs(input.number_of_packs) {
        return Err(NumberOfPacksBelowZero);
    }

    let batch_pair = check_batch_exists_option(store_id, input, line_row, connection)?;

    let item = line.item_row.clone();

    if !check_batch_on_hold(&batch_pair.main_batch) {
        return Err(BatchIsOnHold);
    }
    check_location_on_hold(&batch_pair.main_batch).map_err(|e| match e {
        LocationIsOnHoldError::LocationIsOnHold => LocationIsOnHold,
    })?;
    check_reduction_below_zero(input, line_row, &batch_pair)?;

    Ok((line.invoice_line_row, item, batch_pair, invoice))
}

fn check_reduction_below_zero(
    input: &UpdateStockOutLine,
    line: &InvoiceLineRow,
    batch_pair: &BatchPair,
) -> Result<(), UpdateStockOutLineError> {
    // If previous batch is present, this means we are adjust new batch thus:
    // - check full number of pack in invoice
    let reduction = batch_pair.get_main_batch_reduction(input, line);

    if batch_pair
        .main_batch
        .stock_line_row
        .available_number_of_packs
        < reduction
    {
        Err(UpdateStockOutLineError::ReductionBelowZero {
            stock_line_id: batch_pair.main_batch.stock_line_row.id.clone(),
            line_id: line.id.clone(),
        })
    } else {
        Ok(())
    }
}

fn check_batch_exists_option(
    store_id: &str,
    input: &UpdateStockOutLine,
    existing_line: &InvoiceLineRow,
    connection: &StorageConnection,
) -> Result<BatchPair, UpdateStockOutLineError> {
    use UpdateStockOutLineError::*;

    let previous_batch = if let Some(batch_id) = &existing_line.stock_line_id {
        // Should always be found due to constraints on database
        check_batch_exists(store_id, batch_id, connection)?.ok_or(StockLineNotFound)?
    } else {
        // This should never happen, but still need to cover
        return Err(LineDoesNotReferenceStockLine);
    };

    let result = match &input.stock_line_id {
        Some(batch_id) if batch_id != &previous_batch.stock_line_row.id => BatchPair {
            // stock_line changed
            main_batch: check_batch_exists(store_id, batch_id, connection)?
                .ok_or(StockLineNotFound)?,
            previous_batch_option: Some(previous_batch),
        },
        _ => {
            // stock_line_id not changed
            BatchPair {
                main_batch: previous_batch,
                previous_batch_option: None,
            }
        }
    };

    Ok(result)
}
