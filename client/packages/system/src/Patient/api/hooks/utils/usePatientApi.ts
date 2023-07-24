import {
  useGql,
  useAuthContext,
  SortBy,
  CentralPatientSearchInput,
  PatientSearchInput,
} from '@openmsupply-client/common';
import { getPatientQueries, ListParams } from '../../api';
import { getSdk, PatientRowFragment } from '../../operations.generated';

export const usePatientApi = () => {
  const { storeId } = useAuthContext();
  const keys = {
    base: () => ['patient'] as const,
    detail: (id: string) => [...keys.base(), storeId, id] as const,
    history: (id: string) => [...keys.base(), 'history', storeId, id] as const,
    list: () => [...keys.base(), storeId, 'list'] as const,
    paramList: (params: ListParams) => [...keys.list(), params] as const,
    sortedList: (sortBy: SortBy<PatientRowFragment>) =>
      [...keys.list(), sortBy] as const,
    search: (params: PatientSearchInput) =>
      [...keys.list(), 'earch', params] as const,
    centralSearch: (params: CentralPatientSearchInput) =>
      [...keys.list(), 'centralSearch', params] as const,
  };
  const { client } = useGql();
  const queries = getPatientQueries(getSdk(client), storeId);

  return { ...queries, storeId, keys };
};