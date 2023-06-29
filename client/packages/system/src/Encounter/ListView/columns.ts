import {
  useColumns,
  ColumnAlign,
  Column,
  ColumnDescription,
  SortBy,
} from '@openmsupply-client/common';
import { useFormatDateTime } from '@common/intl';
import {
  EncounterRowFragment,
  useDocumentRegistry,
} from '@openmsupply-client/programs';
import {
  getAdditionalInformationColumn,
  getEncounterEventCell,
} from './AdditionalInformationColumn';

interface useEncounterListColumnsProps {
  onChangeSortBy: (column: Column<any>) => void;
  sortBy: SortBy<EncounterRowFragment>;
  includePatient?: boolean;
}

export const useEncounterListColumns = ({
  onChangeSortBy,
  sortBy,
  includePatient = false,
}: useEncounterListColumnsProps) => {
  const { localisedDate, localisedTime } = useFormatDateTime();
  const { data: documentRegistries } =
    useDocumentRegistry.get.documentRegistries();
  includePatient;

  const columnList: ColumnDescription<EncounterRowFragment>[] = [
    {
      key: 'encounter-type',
      label: 'label.encounter-type',
      sortable: false,
      accessor: ({ rowData }) => rowData?.document.documentRegistry?.name,
    },
    {
      key: 'program',
      label: 'label.program',
      accessor: ({ rowData }) => {
        return documentRegistries?.nodes?.find(
          node => node.documentType === rowData.program
        )?.name;
      },
    },
    {
      key: 'startDatetime',
      label: 'label.date',
      accessor: ({ rowData }) => rowData?.startDatetime,
      formatter: dateString =>
        dateString ? localisedDate((dateString as string) || '') : '',
    },
    {
      key: 'startTime',
      label: 'label.encounter-start',
      sortable: false,
      accessor: ({ rowData }) => rowData?.startDatetime,
      formatter: dateString =>
        dateString ? localisedTime((dateString as string) || '') : '',
    },
    {
      key: 'endDatetime',
      label: 'label.encounter-end',
      formatter: dateString =>
        dateString ? localisedTime((dateString as string) || '') : '',
    },
  ];
  if (includePatient)
    columnList.push({
      key: 'patientId',
      label: 'label.patient',
      accessor: ({ rowData }) => rowData?.patient?.name,
    });
  columnList.push(getAdditionalInformationColumn(getEncounterEventCell));
  columnList.push({
    key: 'effectiveStatus',
    label: 'label.status',
    sortable: false,
    align: ColumnAlign.Right,
    width: 175,
  });

  const columns = useColumns<EncounterRowFragment>(
    columnList,
    { onChangeSortBy, sortBy },
    [sortBy]
  );

  return columns;
};
