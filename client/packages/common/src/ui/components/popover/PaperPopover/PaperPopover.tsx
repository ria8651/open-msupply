import React, { FC } from 'react';
import { HoverPopover } from '../HoverPopover';
import { useTranslation, LocaleKey } from '../../../../intl';
import Box from '@mui/material/Box';
import Typography from '@mui/material/Typography';
import Paper, { PaperProps } from '@mui/material/Paper';

interface PaperPopoverProps {
  Content: React.ReactElement<any, any>;
  paperProps?: PaperProps;
  width?: number;
  height?: number;
  placement?: 'top' | 'bottom' | 'left' | 'right';
}

export const PaperPopover: FC<PaperPopoverProps> = ({
  children,
  Content,
  paperProps,
  width = 250,
  height = 125,
  placement = 'left',
}) => {
  return (
    <HoverPopover
      placement={placement}
      Content={
        <Paper
          sx={{
            width,
            height,
            boxShadow: theme => theme.shadows[7],
            ...paperProps?.sx,
          }}
          {...paperProps}
        >
          {Content}
        </Paper>
      }
    >
      {children}
    </HoverPopover>
  );
};

export interface PaperPopoverSectionProps {
  labelKey?: LocaleKey;
}

export const PaperPopoverSection: FC<PaperPopoverSectionProps> = ({
  children,
  labelKey,
}) => {
  const t = useTranslation();

  return (
    <Box gap={2} p={3} flexDirection="column" display="flex">
      <Typography fontWeight="700">{t(labelKey)}</Typography>
      {children}
    </Box>
  );
};
