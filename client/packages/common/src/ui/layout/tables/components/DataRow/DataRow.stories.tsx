import React from 'react';
import { ComponentMeta, Story } from '@storybook/react';
import { TableBody, Table } from '@mui/material';
import { DataRow } from './DataRow';
import { useColumns } from '../../hooks';
import { ColumnSetBuilder } from '../../utils';

export default {
  title: 'Table/DataRow',
  component: DataRow,
  argTypes: {
    backgroundColor: { control: 'color' },
  },
} as ComponentMeta<typeof DataRow>;

const exampleColumns = new ColumnSetBuilder()
  .addColumn('type')
  .addColumn('status')
  .addColumn('comment')
  .build();

const Template: Story = ({ onClick }) => {
  const columns = useColumns(exampleColumns);

  return (
    <Table>
      <TableBody>
        <DataRow
          columns={columns}
          rowKey="rowKey"
          rowData={{
            id: '',
            comment: 'Restock from surge of patients',
            status: 'Finalised',
            type: 'Supplier invoice',
          }}
          onClick={onClick}
        />
      </TableBody>
    </Table>
  );
};

export const Basic = Template.bind({});
Basic.args = {
  onClick: null,
};

export const WithRowClick = Template.bind({});
WithRowClick.args = {
  onClick: () => {},
};