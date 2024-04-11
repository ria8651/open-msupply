import React, { FC } from 'react';
import {
  // useNavigate,
  TableProvider,
  DataTable,
  useColumns,
  createTableStore,
  NothingHere,
  useTranslation,
  useUrlQueryParams,
} from '@openmsupply-client/common';
import { AssetLogReasonFragment, useAssetData } from '../api';

// import { Toolbar } from './Toolbar';
// import { AppBarButtons } from './AppBarButtons';

const AssetListComponent: FC = () => {
  const {
    updateSortQuery,
    updatePaginationQuery,
    queryParams: { sortBy, page, first, offset },
  } = useUrlQueryParams({
    initialSort: { key: 'code', dir: 'asc' },
    filters: [
      { key: 'manufacturer' },
      { key: 'model' },
      { key: 'category' },
      { key: 'class' },
      { key: 'type' },
    ],
  });

  const { data, isError, isLoading } = useAssetData.log.listReasons();
  const pagination = { page, first, offset };
  // const navigate = useNavigate();
  const t = useTranslation('catalogue');

  const columns = useColumns<AssetLogReasonFragment>(
    [
      {
        key: 'reason',
        label: 'label.reason',
        sortable: false,
        accessor: ({ rowData }) => rowData.reason,
      },
      {
        key: 'status',
        label: 'label.status',
        sortable: false,
        accessor: ({ rowData }) => rowData.assetLogStatus,
      },
    ],
    {
      sortBy,
      onChangeSortBy: updateSortQuery,
    },
    [sortBy]
  );

  return (
    <>
      {/* <AppBarButtons />
      <Toolbar /> */}
      <DataTable
        id="item-list"
        pagination={{ ...pagination, total: data?.totalCount ?? 0 }}
        onChangePage={updatePaginationQuery}
        columns={columns}
        data={data?.nodes}
        isError={isError}
        isLoading={isLoading}
        // onRowClick={row => {
        //   navigate(`/catalogue/assets/${row.id}`);
        // }}
        noDataElement={<NothingHere body={t('error.no-items')} />}
      />
    </>
  );
};

export const AssetLogReasonsListView: FC = () => (
  <TableProvider createStore={createTableStore}>
    <AssetListComponent />
  </TableProvider>
);
