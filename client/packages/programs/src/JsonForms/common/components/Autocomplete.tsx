import React, { useState } from 'react';
import { ControlProps, rankWith, uiTypeIs } from '@jsonforms/core';
import { withJsonFormsControlProps } from '@jsonforms/react';
import {
  Autocomplete as AutocompleteCommon,
  DetailInputWithLabelRow,
} from '@openmsupply-client/common';
import { z } from 'zod';
import { useZodOptionsValidation } from '../hooks/useZodOptionsValidation';
import { DefaultFormRowSx, FORM_LABEL_WIDTH } from '../styleConstants';

type Options = {
  freeText?: boolean;
  values?: string[];
};
const Options: z.ZodType<Options | undefined> = z
  .object({
    freeText: z.boolean().optional(),
    values: z.array(z.string()).optional(),
  })
  .strict()
  .optional();

export const autocompleteTester = rankWith(10, uiTypeIs('Autocomplete'));

type DisplayOption = { label: string };

const UIComponent = (props: ControlProps) => {
  const { data, handleChange, label, path, errors } = props;
  const { errors: zErrors, options: schemaOptions } = useZodOptionsValidation(
    Options,
    props.uischema.options
  );

  const [localData, setLocalData] = useState<string | undefined>(data);

  if (!props.visible) {
    return null;
  }

  // In free text mode we don't get an onChange update when changing focus; do an update manually
  const onClose = () => {
    if (!schemaOptions?.freeText) {
      return;
    }
    handleChange(path, localData);
  };

  const onInputChange = (_event: React.SyntheticEvent, value: string) => {
    if (schemaOptions?.freeText) {
      // set the local data so that we have it in onClose; don't set it otherwise because it would
      // affect the normal behaviour, i.e. the input wouldn't clear if the value is not valid
      setLocalData(value);
    }
  };

  const onChange = (
    _event: React.SyntheticEvent,
    value: DisplayOption | null | string
  ) => {
    let s = '';
    if (typeof value === 'string') {
      // from freeText mode
      s = value;
    } else {
      s = value?.label ?? '';
    }
    if (
      !schemaOptions?.freeText &&
      !(schemaOptions?.values ?? []).includes(s)
    ) {
      setLocalData(undefined);
      handleChange(path, undefined);
    } else {
      setLocalData(s);
      handleChange(path, s === '' ? undefined : s);
    }
  };

  const options: DisplayOption[] = (schemaOptions?.values ?? []).map(
    (option: string) => ({
      label: option,
    })
  );
  return (
    <DetailInputWithLabelRow
      sx={DefaultFormRowSx}
      label={label}
      labelWidthPercentage={FORM_LABEL_WIDTH}
      inputAlignment={'start'}
      Input={
        <AutocompleteCommon
          sx={{
            '.MuiFormControl-root': { minWidth: '100%' },
            flexBasis: '100%',
          }}
          options={options}
          value={{ label: localData ?? '' }}
          // some type problem here, freeSolo seems to have type `undefined`
          freeSolo={schemaOptions?.freeText as undefined}
          onClose={onClose}
          onChange={onChange}
          onInputChange={onInputChange}
          clearable={!props.config?.required}
          inputProps={{
            error: !!zErrors || !!errors,
            helperText: zErrors ?? errors,
            color: 'secondary',
          }}
          isOptionEqualToValue={(option, value) => {
            if (typeof value === 'string') {
              return option.label === value;
            }
            return option.label === value.label;
          }}
          disabled={!props.enabled}
        />
      }
    />
  );
};

export const Autocomplete = withJsonFormsControlProps(UIComponent);