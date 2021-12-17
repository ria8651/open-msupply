import React from 'react';
import { Box } from '@mui/material';
import { DomainObject } from '@common/types';
import { ColumnDefinition } from '../columns/types';
import { ColorSelectButton } from '@common/components';

interface DomainObjectWithRequiredFields extends DomainObject {
  color: string;
  otherPartyName: string;
}

export const getNameAndColorColumn = <
  T extends DomainObjectWithRequiredFields
>(): ColumnDefinition<T> => ({
  label: 'label.name',
  width: 350,
  setter: () => {
    if (process.env['NODE_ENV']) {
      throw new Error(
        `The default setter of the NameAndColor column was called.
        Have you forgotten to provide a custom setter?
        When setting up your columns, you should provide a setter function
        const columns = useColumns([{...getNameAndColorColumn(), setter }])
        `
      );
    }
  },
  accessor: (rowData: T) => rowData.otherPartyName,
  key: 'otherPartyName',
  Cell: ({ rowData, column }) => (
    <Box
      sx={{
        flexDirection: 'row',
        borderBottom: 'none',
        alignItems: 'center',
        display: 'flex',
      }}
    >
      <ColorSelectButton
        onChange={color => column.setter({ ...rowData, color })}
        color={rowData.color}
      />
      <Box ml={1} />
      {column.accessor(rowData)}
    </Box>
  ),
});
