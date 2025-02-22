import React, { FC, useEffect } from 'react';
import {
  AppBarContentPortal,
  Box,
  useTranslation,
  Stack,
  LocaleKey,
  Typography,
  useFormatDateTime,
  DateUtils,
  Formatter,
  useBreadcrumbs,
  useIntlUtils,
} from '@openmsupply-client/common';
import { usePatient } from '../api';
import { AppRoute } from '@openmsupply-client/config';

const SummaryRow = ({ label, value }: { label: LocaleKey; value: string }) => {
  const t = useTranslation('dispensary');
  return (
    <Box gap={1} display="flex">
      <Box style={{ textAlign: 'start', width: 135 }}>
        <Typography sx={{ fontWeight: 'bold' }}>{t(label)}:</Typography>
      </Box>
      <Box flex={1}>
        <Typography>{value}</Typography>
      </Box>
    </Box>
  );
};

export const PatientSummary: FC = () => {
  const patientId = usePatient.utils.id();
  const { data: patient } = usePatient.document.get(patientId);
  const { localisedDate } = useFormatDateTime();
  const { getLocalisedFullName } = useIntlUtils();
  const { setSuffix } = useBreadcrumbs([AppRoute.Patients]);
  const t = useTranslation('dispensary');
  const formatDateOfBirth = (dateOfBirth: string | null) => {
    const dob = DateUtils.getDateOrNull(dateOfBirth);

    return !dob
      ? ''
      : `${localisedDate(dob)} (${t('label.age')}: ${DateUtils.age(dob)})`;
  };
  useEffect(() => {
    if (patient)
      setSuffix(
        `${getLocalisedFullName(patient?.firstName, patient?.lastName)}`
      );
    else setSuffix(t('label.new-patient'));
  }, [patient]);

  return (
    <AppBarContentPortal sx={{ display: 'flex', flex: 1, marginBottom: 1 }}>
      <Stack>
        <SummaryRow
          label="label.patient-id"
          value={String(patient?.code ?? '')}
        />
        <SummaryRow
          label="label.gender"
          value={Formatter.sentenceCase(String(patient?.gender ?? ''))}
        />
        <SummaryRow
          label="label.date-of-birth"
          value={formatDateOfBirth(patient?.dateOfBirth ?? null)}
        />
      </Stack>
    </AppBarContentPortal>
  );
};
