import React from 'react';
import { rankWith, ControlProps, isDateControl } from '@jsonforms/core';
import { withJsonFormsControlProps } from '@jsonforms/react';
import { FormLabel, Box } from '@mui/material';
import {
  BaseDatePickerInput,
  useFormatDateTime,
} from '@openmsupply-client/common';
import {
  FORM_LABEL_COLUMN_WIDTH,
  FORM_INPUT_COLUMN_WIDTH,
} from '../styleConstants';

export const dateTester = rankWith(5, isDateControl);

const UIComponent = (props: ControlProps) => {
  const { data, handleChange, label, path } = props;
  const dateFormatter = useFormatDateTime().customDate;
  return (
    <Box
      display="flex"
      alignItems="center"
      gap={2}
      justifyContent="space-around"
      style={{ minWidth: 300 }}
      marginTop={1}
    >
      <Box
        flex={1}
        style={{ textAlign: 'end' }}
        flexBasis={FORM_LABEL_COLUMN_WIDTH}
      >
        <FormLabel sx={{ fontWeight: 'bold' }}>{label}:</FormLabel>
      </Box>
      <Box flex={1} flexBasis={FORM_INPUT_COLUMN_WIDTH}>
        <BaseDatePickerInput
          value={data}
          onChange={e => {
            if (e) handleChange(path, dateFormatter(e, 'yyyy-MM-dd'));
          }}
          inputFormat="dd/MM/yyyy"
        />
      </Box>
    </Box>
  );
};

export const Date = withJsonFormsControlProps(UIComponent);
