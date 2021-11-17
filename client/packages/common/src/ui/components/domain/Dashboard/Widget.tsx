import React from 'react';
import { Box, CircularProgress, Paper, Typography } from '@mui/material';
import { LocaleKey, useTranslation } from '../../../../intl/intlHelpers';

const Loading = () => (
  <Box display="flex" flex={1} justifyContent="center" alignItems="center">
    <CircularProgress />
  </Box>
);

interface WidgetProps {
  height?: number | string;
  titleKey: LocaleKey;
}

export const Widget: React.FC<WidgetProps> = ({
  children,
  height = '100%',
  titleKey,
}) => {
  const t = useTranslation();
  return (
    <Paper
      sx={{
        borderRadius: '16px',
        height,
        padding: '24px',
        display: 'flex',
        flexDirection: 'column',
        margin: '11px',
        flex: 1,
        boxShadow: theme => theme.shadows[2],
      }}
    >
      <Typography sx={{ fontSize: '14px', fontWeight: 'bold' }}>
        {t(titleKey)}
      </Typography>

      <React.Suspense fallback={<Loading />}>{children}</React.Suspense>
    </Paper>
  );
};