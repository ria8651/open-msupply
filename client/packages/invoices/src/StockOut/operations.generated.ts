import * as Types from '@openmsupply-client/common';

import gql from 'graphql-tag';
export type PartialStockLineFragment = { __typename: 'StockLineNode', id: string, itemId: string, availableNumberOfPacks: number, totalNumberOfPacks: number, onHold: boolean, sellPricePerPack: number, packSize: number, expiryDate?: string | null, item: { __typename: 'ItemNode', name: string, code: string }, location?: { __typename: 'LocationNode', id: string, name: string, code: string, onHold: boolean } | null };

export type StockOutLineFragment = { __typename: 'InvoiceLineNode', id: string, type: Types.InvoiceLineNodeType, batch?: string | null, expiryDate?: string | null, numberOfPacks: number, packSize: number, invoiceId: string, sellPricePerPack: number, note?: string | null, totalBeforeTax: number, totalAfterTax: number, taxPercentage?: number | null, item: { __typename: 'ItemNode', id: string, name: string, code: string, unitName?: string | null }, location?: { __typename: 'LocationNode', id: string, name: string, code: string, onHold: boolean } | null, stockLine?: { __typename: 'StockLineNode', id: string, itemId: string, batch?: string | null, availableNumberOfPacks: number, totalNumberOfPacks: number, onHold: boolean, sellPricePerPack: number, packSize: number, expiryDate?: string | null, item: { __typename: 'ItemNode', name: string, code: string } } | null };

export const PartialStockLineFragmentDoc = gql`
    fragment PartialStockLine on StockLineNode {
  id
  itemId
  availableNumberOfPacks
  totalNumberOfPacks
  onHold
  sellPricePerPack
  packSize
  expiryDate
  item {
    name
    code
  }
  location {
    __typename
    id
    name
    code
    onHold
  }
}
    `;
export const StockOutLineFragmentDoc = gql`
    fragment StockOutLine on InvoiceLineNode {
  __typename
  id
  type
  batch
  expiryDate
  numberOfPacks
  packSize
  invoiceId
  sellPricePerPack
  note
  totalBeforeTax
  totalAfterTax
  taxPercentage
  note
  item {
    __typename
    id
    name
    code
    unitName
  }
  location {
    __typename
    id
    name
    code
    onHold
  }
  stockLine {
    __typename
    id
    itemId
    batch
    availableNumberOfPacks
    totalNumberOfPacks
    onHold
    sellPricePerPack
    packSize
    expiryDate
    item {
      name
      code
    }
  }
}
    `;

export type SdkFunctionWrapper = <T>(action: (requestHeaders?:Record<string, string>) => Promise<T>, operationName: string, operationType?: string) => Promise<T>;
