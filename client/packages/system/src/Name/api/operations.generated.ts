import * as Types from '@openmsupply-client/common';

import { GraphQLClient } from 'graphql-request';
import { GraphQLClientRequestHeaders } from 'graphql-request/build/cjs/types';
import gql from 'graphql-tag';
import { graphql, ResponseResolver, GraphQLRequest, GraphQLContext } from 'msw'
export type NameRowFragment = { __typename: 'NameNode', code: string, id: string, isCustomer: boolean, isSupplier: boolean, isOnHold: boolean, name: string, store?: { __typename: 'StoreNode', id: string, code: string } | null };

export type NameFragment = { __typename: 'NameNode', address1?: string | null, address2?: string | null, chargeCode?: string | null, code: string, comment?: string | null, country?: string | null, createdDatetime?: string | null, email?: string | null, id: string, isCustomer: boolean, isDonor: boolean, isManufacturer: boolean, isOnHold: boolean, isSupplier: boolean, isSystemName: boolean, name: string, phone?: string | null, website?: string | null, properties: string, store?: { __typename: 'StoreNode', id: string, code: string } | null };

export type PropertyFragment = { __typename: 'PropertyNode', id: string, key: string, name: string, allowedValues?: string | null, valueType: Types.PropertyNodeValueType };

export type NamesQueryVariables = Types.Exact<{
  storeId: Types.Scalars['String']['input'];
  key: Types.NameSortFieldInput;
  desc?: Types.InputMaybe<Types.Scalars['Boolean']['input']>;
  first?: Types.InputMaybe<Types.Scalars['Int']['input']>;
  offset?: Types.InputMaybe<Types.Scalars['Int']['input']>;
  filter?: Types.InputMaybe<Types.NameFilterInput>;
}>;


export type NamesQuery = { __typename: 'Queries', names: { __typename: 'NameConnector', totalCount: number, nodes: Array<{ __typename: 'NameNode', code: string, id: string, isCustomer: boolean, isSupplier: boolean, isOnHold: boolean, name: string, store?: { __typename: 'StoreNode', id: string, code: string } | null }> } };

export type NameByIdQueryVariables = Types.Exact<{
  storeId: Types.Scalars['String']['input'];
  nameId: Types.Scalars['String']['input'];
}>;


export type NameByIdQuery = { __typename: 'Queries', names: { __typename: 'NameConnector', totalCount: number, nodes: Array<{ __typename: 'NameNode', address1?: string | null, address2?: string | null, chargeCode?: string | null, code: string, comment?: string | null, country?: string | null, createdDatetime?: string | null, email?: string | null, id: string, isCustomer: boolean, isDonor: boolean, isManufacturer: boolean, isOnHold: boolean, isSupplier: boolean, isSystemName: boolean, name: string, phone?: string | null, website?: string | null, properties: string, store?: { __typename: 'StoreNode', id: string, code: string } | null }> } };

export type NamePropertiesQueryVariables = Types.Exact<{ [key: string]: never; }>;


export type NamePropertiesQuery = { __typename: 'Queries', nameProperties: { __typename: 'NamePropertyConnector', nodes: Array<{ __typename: 'NamePropertyNode', id: string, remoteEditable: boolean, property: { __typename: 'PropertyNode', id: string, key: string, name: string, allowedValues?: string | null, valueType: Types.PropertyNodeValueType } }> } };

export type UpdateNamePropertiesMutationVariables = Types.Exact<{
  storeId: Types.Scalars['String']['input'];
  input?: Types.InputMaybe<Types.UpdateNamePropertiesInput>;
}>;


export type UpdateNamePropertiesMutation = { __typename: 'Mutations', updateNameProperties: { __typename: 'NameNode', address1?: string | null, address2?: string | null, chargeCode?: string | null, code: string, comment?: string | null, country?: string | null, createdDatetime?: string | null, email?: string | null, id: string, isCustomer: boolean, isDonor: boolean, isManufacturer: boolean, isOnHold: boolean, isSupplier: boolean, isSystemName: boolean, name: string, phone?: string | null, website?: string | null, properties: string, store?: { __typename: 'StoreNode', id: string, code: string } | null } | { __typename: 'UpdateNamePropertiesError', error: { __typename: 'RecordNotFound', description: string } } };

export const NameRowFragmentDoc = gql`
    fragment NameRow on NameNode {
  code
  id
  isCustomer
  isSupplier
  isOnHold
  name
  store {
    id
    code
  }
}
    `;
export const NameFragmentDoc = gql`
    fragment Name on NameNode {
  address1
  address2
  chargeCode
  code
  comment
  country
  createdDatetime
  email
  id
  isCustomer
  isDonor
  isManufacturer
  isOnHold
  isSupplier
  isSystemName
  name
  phone
  website
  store {
    id
    code
  }
  properties
}
    `;
export const PropertyFragmentDoc = gql`
    fragment Property on PropertyNode {
  id
  key
  name
  allowedValues
  valueType
}
    `;
export const NamesDocument = gql`
    query names($storeId: String!, $key: NameSortFieldInput!, $desc: Boolean, $first: Int, $offset: Int, $filter: NameFilterInput) {
  names(
    storeId: $storeId
    page: {first: $first, offset: $offset}
    sort: {key: $key, desc: $desc}
    filter: $filter
  ) {
    ... on NameConnector {
      __typename
      nodes {
        ...NameRow
      }
      totalCount
    }
  }
}
    ${NameRowFragmentDoc}`;
export const NameByIdDocument = gql`
    query nameById($storeId: String!, $nameId: String!) {
  names(storeId: $storeId, filter: {id: {equalTo: $nameId}}) {
    ... on NameConnector {
      __typename
      nodes {
        ...Name
      }
      totalCount
    }
  }
}
    ${NameFragmentDoc}`;
export const NamePropertiesDocument = gql`
    query nameProperties {
  nameProperties {
    ... on NamePropertyConnector {
      __typename
      nodes {
        __typename
        id
        remoteEditable
        property {
          ...Property
        }
      }
    }
  }
}
    ${PropertyFragmentDoc}`;
export const UpdateNamePropertiesDocument = gql`
    mutation updateNameProperties($storeId: String!, $input: UpdateNamePropertiesInput) {
  updateNameProperties(storeId: $storeId, input: $input) {
    __typename
    ... on NameNode {
      ...Name
    }
    ... on UpdateNamePropertiesError {
      error {
        __typename
        description
      }
    }
  }
}
    ${NameFragmentDoc}`;

export type SdkFunctionWrapper = <T>(action: (requestHeaders?:Record<string, string>) => Promise<T>, operationName: string, operationType?: string) => Promise<T>;


const defaultWrapper: SdkFunctionWrapper = (action, _operationName, _operationType) => action();

export function getSdk(client: GraphQLClient, withWrapper: SdkFunctionWrapper = defaultWrapper) {
  return {
    names(variables: NamesQueryVariables, requestHeaders?: GraphQLClientRequestHeaders): Promise<NamesQuery> {
      return withWrapper((wrappedRequestHeaders) => client.request<NamesQuery>(NamesDocument, variables, {...requestHeaders, ...wrappedRequestHeaders}), 'names', 'query');
    },
    nameById(variables: NameByIdQueryVariables, requestHeaders?: GraphQLClientRequestHeaders): Promise<NameByIdQuery> {
      return withWrapper((wrappedRequestHeaders) => client.request<NameByIdQuery>(NameByIdDocument, variables, {...requestHeaders, ...wrappedRequestHeaders}), 'nameById', 'query');
    },
    nameProperties(variables?: NamePropertiesQueryVariables, requestHeaders?: GraphQLClientRequestHeaders): Promise<NamePropertiesQuery> {
      return withWrapper((wrappedRequestHeaders) => client.request<NamePropertiesQuery>(NamePropertiesDocument, variables, {...requestHeaders, ...wrappedRequestHeaders}), 'nameProperties', 'query');
    },
    updateNameProperties(variables: UpdateNamePropertiesMutationVariables, requestHeaders?: GraphQLClientRequestHeaders): Promise<UpdateNamePropertiesMutation> {
      return withWrapper((wrappedRequestHeaders) => client.request<UpdateNamePropertiesMutation>(UpdateNamePropertiesDocument, variables, {...requestHeaders, ...wrappedRequestHeaders}), 'updateNameProperties', 'mutation');
    }
  };
}
export type Sdk = ReturnType<typeof getSdk>;

/**
 * @param resolver a function that accepts a captured request and may return a mocked response.
 * @see https://mswjs.io/docs/basics/response-resolver
 * @example
 * mockNamesQuery((req, res, ctx) => {
 *   const { storeId, key, desc, first, offset, filter } = req.variables;
 *   return res(
 *     ctx.data({ names })
 *   )
 * })
 */
export const mockNamesQuery = (resolver: ResponseResolver<GraphQLRequest<NamesQueryVariables>, GraphQLContext<NamesQuery>, any>) =>
  graphql.query<NamesQuery, NamesQueryVariables>(
    'names',
    resolver
  )

/**
 * @param resolver a function that accepts a captured request and may return a mocked response.
 * @see https://mswjs.io/docs/basics/response-resolver
 * @example
 * mockNameByIdQuery((req, res, ctx) => {
 *   const { storeId, nameId } = req.variables;
 *   return res(
 *     ctx.data({ names })
 *   )
 * })
 */
export const mockNameByIdQuery = (resolver: ResponseResolver<GraphQLRequest<NameByIdQueryVariables>, GraphQLContext<NameByIdQuery>, any>) =>
  graphql.query<NameByIdQuery, NameByIdQueryVariables>(
    'nameById',
    resolver
  )

/**
 * @param resolver a function that accepts a captured request and may return a mocked response.
 * @see https://mswjs.io/docs/basics/response-resolver
 * @example
 * mockNamePropertiesQuery((req, res, ctx) => {
 *   return res(
 *     ctx.data({ nameProperties })
 *   )
 * })
 */
export const mockNamePropertiesQuery = (resolver: ResponseResolver<GraphQLRequest<NamePropertiesQueryVariables>, GraphQLContext<NamePropertiesQuery>, any>) =>
  graphql.query<NamePropertiesQuery, NamePropertiesQueryVariables>(
    'nameProperties',
    resolver
  )

/**
 * @param resolver a function that accepts a captured request and may return a mocked response.
 * @see https://mswjs.io/docs/basics/response-resolver
 * @example
 * mockUpdateNamePropertiesMutation((req, res, ctx) => {
 *   const { storeId, input } = req.variables;
 *   return res(
 *     ctx.data({ updateNameProperties })
 *   )
 * })
 */
export const mockUpdateNamePropertiesMutation = (resolver: ResponseResolver<GraphQLRequest<UpdateNamePropertiesMutationVariables>, GraphQLContext<UpdateNamePropertiesMutation>, any>) =>
  graphql.mutation<UpdateNamePropertiesMutation, UpdateNamePropertiesMutationVariables>(
    'updateNameProperties',
    resolver
  )
