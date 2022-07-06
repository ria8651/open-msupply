import { useQuery, useUrlQueryParams } from '@openmsupply-client/common';
import { useItemApi } from '../useItemApi';

export const useItems = () => {
  const { queryParams } = useUrlQueryParams({ initialSort: 'name' });
  const api = useItemApi();

  return {
    ...useQuery(api.keys.paramList(queryParams), () =>
      api.get.list(queryParams)
    ),
  };
};
