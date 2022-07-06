import { useQuery, useUrlQueryParams } from '@openmsupply-client/common';
import { useNameApi } from '../utils/useNameApi';

export const useNames = (type: 'customer' | 'supplier') => {
  const { queryParams } = useUrlQueryParams({
    initialSort: 'name',
  });
  const api = useNameApi();
  return {
    ...useQuery(api.keys.paramList(queryParams), () =>
      api.get.list({
        first: queryParams.first,
        offset: queryParams.offset,
        sortBy: queryParams.sortBy,
        type: type === 'customer' ? 'customer' : 'supplier',
      })
    ),
  };
};
