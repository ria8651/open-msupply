import { useGql, useAuthContext } from '@openmsupply-client/common';
import { getProgramEnrolmentQueries } from '../../api';
import { getSdk } from '../../operations.generated';

export const useProgramEnrolmentApi = () => {
  const { storeId } = useAuthContext();
  const keys = {
    base: () => ['program-enrolment'] as const,
  };
  const { client } = useGql();
  const queries = getProgramEnrolmentQueries(getSdk(client), storeId);

  return { ...queries, storeId, keys };
};
