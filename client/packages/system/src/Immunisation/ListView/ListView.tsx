import React, { FC } from 'react';
import {
  TableProvider,
  DataTable,
  useColumns,
  useUrlQueryParams,
  useNavigate,
  NothingHere,
  useTranslation,
  createTableStore,
  createQueryParamsStore,
  useEditModal,
} from '@openmsupply-client/common';
import { Toolbar } from './Toolbar';
import { AppBarButtons } from './AppBarButtons';
import { ImmunisationProgramCreateModal } from './ImmunisationProgramCreateModal';
import { useImmunisationProgramList } from '../api/hooks/useImmunisationProgramList';

export interface Program {
  id: string;
  name: string;
  immunisations: string[];
  isNew: boolean;
}

const ProgramListComponent: FC = () => {
  const {
    updateSortQuery,
    updatePaginationQuery,
    queryParams: { sortBy, page, first, offset, filterBy },
  } = useUrlQueryParams({ filters: [{ key: 'name' }] });
  const pagination = { page, first, offset };
  const navigate = useNavigate();
  const t = useTranslation('coldchain');

  const queryParams = {
    filterBy,
    offset,
    sortBy,
    first,
  };
  const { data, isLoading, isError } = useImmunisationProgramList(queryParams);

  const columns = useColumns(
    ['name'],
    {
      onChangeSortBy: updateSortQuery,
      sortBy,
    },
    [updateSortQuery, sortBy]
  );

  // later create modal will use <InsertImmunisationProgram> type
  const { isOpen, onClose, onOpen } = useEditModal<any>();

  return (
    <>
      {isOpen && (
        <ImmunisationProgramCreateModal isOpen={isOpen} onClose={onClose} />
      )}
      <Toolbar />
      <AppBarButtons onCreate={onOpen} />
      <DataTable
        id={'immunisation-list'}
        pagination={{ ...pagination }}
        onChangePage={updatePaginationQuery}
        columns={columns}
        data={data?.nodes ?? []}
        isLoading={isLoading}
        isError={isError}
        onRowClick={row => navigate(row.id)}
        noDataElement={
          <NothingHere body={t('error.no-immunisation-programs')} />
        }
      />
    </>
  );
};

export const ProgramListView: FC = () => (
  <TableProvider
    createStore={createTableStore}
    queryParamsStore={createQueryParamsStore({
      initialSortBy: { key: 'name' },
    })}
  >
    <ProgramListComponent />
  </TableProvider>
);
