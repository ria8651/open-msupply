import React, { FC } from 'react';
import {
  DataTable,
  useTranslation,
  Box,
  Switch,
  useIsGrouped,
  useSortBy,
  getDataSorter,
} from '@openmsupply-client/common';
import { useStocktakeColumns } from './columns';
import { useStocktakeLines, useStocktakeItems } from '../api';
import { StocktakeSummaryItem, StocktakeLine } from '../../types';

const Expand: FC<{ rowData: StocktakeSummaryItem | StocktakeLine }> = ({
  rowData,
}) => {
  return (
    <Box p={1} height={300} style={{ overflow: 'scroll' }}>
      <Box
        flex={1}
        display="flex"
        height="100%"
        borderRadius={4}
        bgcolor="#c7c9d933"
      >
        <span style={{ whiteSpace: 'pre-wrap' }}>
          {JSON.stringify(rowData, null, 2)}
        </span>
      </Box>
    </Box>
  );
};

const useStocktakeRows = (isGrouped: boolean) => {
  const { sortBy, onChangeSortBy } = useSortBy<
    StocktakeLine | StocktakeSummaryItem
  >({
    key: 'itemName',
  });
  const { data: lines } = useStocktakeLines();
  const { data: items } = useStocktakeItems();

  const rows = isGrouped
    ? items?.sort(
        getDataSorter(sortBy.key as keyof StocktakeSummaryItem, !!sortBy.isDesc)
      )
    : lines?.sort(
        getDataSorter(sortBy.key as keyof StocktakeLine, !!sortBy.isDesc)
      );

  return { rows, onChangeSortBy, sortBy };
};

export const ContentArea: FC<{
  onRowClick: (item: StocktakeSummaryItem | StocktakeLine) => void;
}> = ({ onRowClick }) => {
  const t = useTranslation('inventory');
  const { isGrouped, toggleIsGrouped } = useIsGrouped('inboundShipment');
  const { rows, onChangeSortBy, sortBy } = useStocktakeRows(isGrouped);
  const columns = useStocktakeColumns({ onChangeSortBy, sortBy });

  if (!rows) return null;

  return (
    <Box flexDirection="column" flex={1}>
      <Box style={{ padding: 5, marginInlineStart: 15 }}>
        <Switch
          label={t('label.group-by-item')}
          onChange={toggleIsGrouped}
          checked={isGrouped}
          size="small"
          disabled={rows?.length === 0}
          color="secondary"
        />
      </Box>
      <DataTable<StocktakeSummaryItem | StocktakeLine>
        onRowClick={onRowClick}
        ExpandContent={Expand}
        columns={columns}
        data={rows}
        noDataMessage={t('error.no-items')}
      />
    </Box>
  );
};