import React, { FC } from 'react';
import {
  DataTable,
  ObjectWithStringKeys,
  usePagination,
  Column,
  DomainObject,
  Box,
  useTranslation,
} from '@openmsupply-client/common';
import { InboundShipmentItem } from '../../types';

interface GeneralTabProps<T extends ObjectWithStringKeys & DomainObject> {
  data: T[];
  columns: Column<T>[];
  onRowClick?: (rowData: T) => void;
}

const Expand: FC<{ rowData: InboundShipmentItem }> = ({ rowData }) => {
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

export const GeneralTabComponent: FC<GeneralTabProps<InboundShipmentItem>> = ({
  data,
  columns,
  onRowClick,
}) => {
  const { pagination } = usePagination();
  const activeRows = data.filter(({ isDeleted }) => !isDeleted);
  const t = useTranslation('common');

  return (
    <DataTable
      onRowClick={onRowClick}
      ExpandContent={Expand}
      pagination={{ ...pagination, total: activeRows.length }}
      columns={columns}
      data={activeRows.slice(
        pagination.offset,
        pagination.offset + pagination.first
      )}
      onChangePage={pagination.onChangePage}
      noDataMessage={t('error.no-items')}
    />
  );
};

export const GeneralTab = React.memo(GeneralTabComponent);