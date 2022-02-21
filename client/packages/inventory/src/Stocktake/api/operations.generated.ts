import * as Types from '@openmsupply-client/common';

import { GraphQLClient } from 'graphql-request';
import * as Dom from 'graphql-request/dist/types.dom';
import gql from 'graphql-tag';
import { graphql, ResponseResolver, GraphQLRequest, GraphQLContext } from 'msw'
export type StocktakeRowFragment = { __typename: 'StocktakeNode', id: string, comment?: string | null, description?: string | null, createdDatetime: any, stocktakeNumber: number, status: Types.StocktakeNodeStatus };

export type StocktakeLineFragment = { __typename: 'StocktakeLineNode', batch?: string | null, itemId: string, id: string, expiryDate?: string | null, packSize?: number | null, snapshotNumberOfPacks: number, countedNumberOfPacks?: number | null, sellPricePerPack?: number | null, costPricePerPack?: number | null };

export type StocktakeFragment = { __typename: 'StocktakeNode', id: string, stocktakeNumber: number, comment?: string | null, createdDatetime: any, status: Types.StocktakeNodeStatus, description?: string | null, lines: { __typename: 'StocktakeLineConnector', totalCount: number, nodes: Array<{ __typename: 'StocktakeLineNode', batch?: string | null, itemId: string, id: string, expiryDate?: string | null, packSize?: number | null, snapshotNumberOfPacks: number, countedNumberOfPacks?: number | null, sellPricePerPack?: number | null, costPricePerPack?: number | null }> } };

export type StocktakesQueryVariables = Types.Exact<{
  storeId: Types.Scalars['String'];
  filter?: Types.InputMaybe<Types.StocktakeFilterInput>;
  page?: Types.InputMaybe<Types.PaginationInput>;
  sort?: Types.InputMaybe<Array<Types.StocktakeSortInput> | Types.StocktakeSortInput>;
}>;


export type StocktakesQuery = { __typename: 'Queries', stocktakes: { __typename: 'StocktakeConnector', totalCount: number, nodes: Array<{ __typename: 'StocktakeNode', id: string, comment?: string | null, description?: string | null, createdDatetime: any, stocktakeNumber: number, status: Types.StocktakeNodeStatus }> } };

export type StocktakeQueryVariables = Types.Exact<{
  stocktakeId: Types.Scalars['String'];
  storeId: Types.Scalars['String'];
}>;


export type StocktakeQuery = { __typename: 'Queries', stocktake: { __typename: 'NodeError' } | { __typename: 'StocktakeNode', id: string, stocktakeNumber: number, comment?: string | null, createdDatetime: any, status: Types.StocktakeNodeStatus, description?: string | null, lines: { __typename: 'StocktakeLineConnector', totalCount: number, nodes: Array<{ __typename: 'StocktakeLineNode', batch?: string | null, itemId: string, id: string, expiryDate?: string | null, packSize?: number | null, snapshotNumberOfPacks: number, countedNumberOfPacks?: number | null, sellPricePerPack?: number | null, costPricePerPack?: number | null }> } } };

export type UpsertStocktakeLinesMutationVariables = Types.Exact<{
  storeId: Types.Scalars['String'];
  deleteStocktakeLines?: Types.InputMaybe<Array<Types.DeleteStocktakeLineInput> | Types.DeleteStocktakeLineInput>;
  updateStocktakeLines?: Types.InputMaybe<Array<Types.UpdateStocktakeLineInput> | Types.UpdateStocktakeLineInput>;
  insertStocktakeLines?: Types.InputMaybe<Array<Types.InsertStocktakeLineInput> | Types.InsertStocktakeLineInput>;
}>;


export type UpsertStocktakeLinesMutation = { __typename: 'Mutations', batchStocktake: { __typename: 'BatchStocktakeResponses', deleteStocktakeLines?: Array<{ __typename: 'DeleteStocktakeLineResponseWithId', id: string }> | null, insertStocktakeLines?: Array<{ __typename: 'InsertStocktakeLineResponseWithId', id: string }> | null, updateStocktakeLines?: Array<{ __typename: 'UpdateStocktakeLineResponseWithId', id: string }> | null } | { __typename: 'BatchStocktakeResponsesWithErrors' } };

export type DeleteStocktakesMutationVariables = Types.Exact<{
  storeId: Types.Scalars['String'];
  ids?: Types.InputMaybe<Array<Types.DeleteStocktakeInput> | Types.DeleteStocktakeInput>;
}>;


export type DeleteStocktakesMutation = { __typename: 'Mutations', batchStocktake: { __typename: 'BatchStocktakeResponses', deleteStocktakes?: Array<{ __typename: 'DeleteStocktakeResponseWithId', id: string }> | null } | { __typename: 'BatchStocktakeResponsesWithErrors' } };

export type UpdateStocktakeMutationVariables = Types.Exact<{
  input: Types.UpdateStocktakeInput;
  storeId: Types.Scalars['String'];
}>;


export type UpdateStocktakeMutation = { __typename: 'Mutations', updateStocktake: { __typename: 'StocktakeNode', id: string } | { __typename: 'UpdateStocktakeError' } };

export type InsertStocktakeMutationVariables = Types.Exact<{
  input: Types.InsertStocktakeInput;
  storeId: Types.Scalars['String'];
}>;


export type InsertStocktakeMutation = { __typename: 'Mutations', insertStocktake: { __typename: 'StocktakeNode', id: string } };

export const StocktakeRowFragmentDoc = gql`
    fragment StocktakeRow on StocktakeNode {
  __typename
  id
  comment
  description
  createdDatetime
  stocktakeNumber
  status
}
    `;
export const StocktakeLineFragmentDoc = gql`
    fragment StocktakeLine on StocktakeLineNode {
  __typename
  batch
  itemId
  id
  expiryDate
  packSize
  snapshotNumberOfPacks
  countedNumberOfPacks
  sellPricePerPack
  costPricePerPack
}
    `;
export const StocktakeFragmentDoc = gql`
    fragment Stocktake on StocktakeNode {
  __typename
  id
  stocktakeNumber
  comment
  createdDatetime
  status
  description
  lines {
    __typename
    ... on StocktakeLineConnector {
      __typename
      nodes {
        ...StocktakeLine
      }
      totalCount
    }
  }
}
    ${StocktakeLineFragmentDoc}`;
export const StocktakesDocument = gql`
    query stocktakes($storeId: String!, $filter: StocktakeFilterInput, $page: PaginationInput, $sort: [StocktakeSortInput!]) {
  stocktakes(storeId: $storeId, filter: $filter, page: $page, sort: $sort) {
    __typename
    ... on StocktakeConnector {
      nodes {
        ...StocktakeRow
      }
      totalCount
    }
  }
}
    ${StocktakeRowFragmentDoc}`;
export const StocktakeDocument = gql`
    query stocktake($stocktakeId: String!, $storeId: String!) {
  stocktake(id: $stocktakeId, storeId: $storeId) {
    __typename
    ... on StocktakeNode {
      ...Stocktake
    }
  }
}
    ${StocktakeFragmentDoc}`;
export const UpsertStocktakeLinesDocument = gql`
    mutation upsertStocktakeLines($storeId: String!, $deleteStocktakeLines: [DeleteStocktakeLineInput!], $updateStocktakeLines: [UpdateStocktakeLineInput!], $insertStocktakeLines: [InsertStocktakeLineInput!]) {
  batchStocktake(
    storeId: $storeId
    input: {deleteStocktakeLines: $deleteStocktakeLines, updateStocktakeLines: $updateStocktakeLines, insertStocktakeLines: $insertStocktakeLines}
  ) {
    __typename
    ... on BatchStocktakeResponses {
      __typename
      deleteStocktakeLines {
        id
      }
      insertStocktakeLines {
        id
      }
      updateStocktakeLines {
        id
      }
    }
  }
}
    `;
export const DeleteStocktakesDocument = gql`
    mutation deleteStocktakes($storeId: String!, $ids: [DeleteStocktakeInput!]) {
  batchStocktake(storeId: $storeId, input: {deleteStocktakes: $ids}) {
    __typename
    ... on BatchStocktakeResponses {
      deleteStocktakes {
        __typename
        id
      }
    }
  }
}
    `;
export const UpdateStocktakeDocument = gql`
    mutation updateStocktake($input: UpdateStocktakeInput!, $storeId: String!) {
  updateStocktake(input: $input, storeId: $storeId) {
    ... on StocktakeNode {
      __typename
      id
    }
  }
}
    `;
export const InsertStocktakeDocument = gql`
    mutation insertStocktake($input: InsertStocktakeInput!, $storeId: String!) {
  insertStocktake(input: $input, storeId: $storeId) {
    ... on StocktakeNode {
      __typename
      id
    }
  }
}
    `;

export type SdkFunctionWrapper = <T>(action: (requestHeaders?:Record<string, string>) => Promise<T>, operationName: string) => Promise<T>;


const defaultWrapper: SdkFunctionWrapper = (action, _operationName) => action();

export function getSdk(client: GraphQLClient, withWrapper: SdkFunctionWrapper = defaultWrapper) {
  return {
    stocktakes(variables: StocktakesQueryVariables, requestHeaders?: Dom.RequestInit["headers"]): Promise<StocktakesQuery> {
      return withWrapper((wrappedRequestHeaders) => client.request<StocktakesQuery>(StocktakesDocument, variables, {...requestHeaders, ...wrappedRequestHeaders}), 'stocktakes');
    },
    stocktake(variables: StocktakeQueryVariables, requestHeaders?: Dom.RequestInit["headers"]): Promise<StocktakeQuery> {
      return withWrapper((wrappedRequestHeaders) => client.request<StocktakeQuery>(StocktakeDocument, variables, {...requestHeaders, ...wrappedRequestHeaders}), 'stocktake');
    },
    upsertStocktakeLines(variables: UpsertStocktakeLinesMutationVariables, requestHeaders?: Dom.RequestInit["headers"]): Promise<UpsertStocktakeLinesMutation> {
      return withWrapper((wrappedRequestHeaders) => client.request<UpsertStocktakeLinesMutation>(UpsertStocktakeLinesDocument, variables, {...requestHeaders, ...wrappedRequestHeaders}), 'upsertStocktakeLines');
    },
    deleteStocktakes(variables: DeleteStocktakesMutationVariables, requestHeaders?: Dom.RequestInit["headers"]): Promise<DeleteStocktakesMutation> {
      return withWrapper((wrappedRequestHeaders) => client.request<DeleteStocktakesMutation>(DeleteStocktakesDocument, variables, {...requestHeaders, ...wrappedRequestHeaders}), 'deleteStocktakes');
    },
    updateStocktake(variables: UpdateStocktakeMutationVariables, requestHeaders?: Dom.RequestInit["headers"]): Promise<UpdateStocktakeMutation> {
      return withWrapper((wrappedRequestHeaders) => client.request<UpdateStocktakeMutation>(UpdateStocktakeDocument, variables, {...requestHeaders, ...wrappedRequestHeaders}), 'updateStocktake');
    },
    insertStocktake(variables: InsertStocktakeMutationVariables, requestHeaders?: Dom.RequestInit["headers"]): Promise<InsertStocktakeMutation> {
      return withWrapper((wrappedRequestHeaders) => client.request<InsertStocktakeMutation>(InsertStocktakeDocument, variables, {...requestHeaders, ...wrappedRequestHeaders}), 'insertStocktake');
    }
  };
}
export type Sdk = ReturnType<typeof getSdk>;

/**
 * @param resolver a function that accepts a captured request and may return a mocked response.
 * @see https://mswjs.io/docs/basics/response-resolver
 * @example
 * mockStocktakesQuery((req, res, ctx) => {
 *   const { storeId, filter, page, sort } = req.variables;
 *   return res(
 *     ctx.data({ stocktakes })
 *   )
 * })
 */
export const mockStocktakesQuery = (resolver: ResponseResolver<GraphQLRequest<StocktakesQueryVariables>, GraphQLContext<StocktakesQuery>, any>) =>
  graphql.query<StocktakesQuery, StocktakesQueryVariables>(
    'stocktakes',
    resolver
  )

/**
 * @param resolver a function that accepts a captured request and may return a mocked response.
 * @see https://mswjs.io/docs/basics/response-resolver
 * @example
 * mockStocktakeQuery((req, res, ctx) => {
 *   const { stocktakeId, storeId } = req.variables;
 *   return res(
 *     ctx.data({ stocktake })
 *   )
 * })
 */
export const mockStocktakeQuery = (resolver: ResponseResolver<GraphQLRequest<StocktakeQueryVariables>, GraphQLContext<StocktakeQuery>, any>) =>
  graphql.query<StocktakeQuery, StocktakeQueryVariables>(
    'stocktake',
    resolver
  )

/**
 * @param resolver a function that accepts a captured request and may return a mocked response.
 * @see https://mswjs.io/docs/basics/response-resolver
 * @example
 * mockUpsertStocktakeLinesMutation((req, res, ctx) => {
 *   const { storeId, deleteStocktakeLines, updateStocktakeLines, insertStocktakeLines } = req.variables;
 *   return res(
 *     ctx.data({ batchStocktake })
 *   )
 * })
 */
export const mockUpsertStocktakeLinesMutation = (resolver: ResponseResolver<GraphQLRequest<UpsertStocktakeLinesMutationVariables>, GraphQLContext<UpsertStocktakeLinesMutation>, any>) =>
  graphql.mutation<UpsertStocktakeLinesMutation, UpsertStocktakeLinesMutationVariables>(
    'upsertStocktakeLines',
    resolver
  )

/**
 * @param resolver a function that accepts a captured request and may return a mocked response.
 * @see https://mswjs.io/docs/basics/response-resolver
 * @example
 * mockDeleteStocktakesMutation((req, res, ctx) => {
 *   const { storeId, ids } = req.variables;
 *   return res(
 *     ctx.data({ batchStocktake })
 *   )
 * })
 */
export const mockDeleteStocktakesMutation = (resolver: ResponseResolver<GraphQLRequest<DeleteStocktakesMutationVariables>, GraphQLContext<DeleteStocktakesMutation>, any>) =>
  graphql.mutation<DeleteStocktakesMutation, DeleteStocktakesMutationVariables>(
    'deleteStocktakes',
    resolver
  )

/**
 * @param resolver a function that accepts a captured request and may return a mocked response.
 * @see https://mswjs.io/docs/basics/response-resolver
 * @example
 * mockUpdateStocktakeMutation((req, res, ctx) => {
 *   const { input, storeId } = req.variables;
 *   return res(
 *     ctx.data({ updateStocktake })
 *   )
 * })
 */
export const mockUpdateStocktakeMutation = (resolver: ResponseResolver<GraphQLRequest<UpdateStocktakeMutationVariables>, GraphQLContext<UpdateStocktakeMutation>, any>) =>
  graphql.mutation<UpdateStocktakeMutation, UpdateStocktakeMutationVariables>(
    'updateStocktake',
    resolver
  )

/**
 * @param resolver a function that accepts a captured request and may return a mocked response.
 * @see https://mswjs.io/docs/basics/response-resolver
 * @example
 * mockInsertStocktakeMutation((req, res, ctx) => {
 *   const { input, storeId } = req.variables;
 *   return res(
 *     ctx.data({ insertStocktake })
 *   )
 * })
 */
export const mockInsertStocktakeMutation = (resolver: ResponseResolver<GraphQLRequest<InsertStocktakeMutationVariables>, GraphQLContext<InsertStocktakeMutation>, any>) =>
  graphql.mutation<InsertStocktakeMutation, InsertStocktakeMutationVariables>(
    'insertStocktake',
    resolver
  )
