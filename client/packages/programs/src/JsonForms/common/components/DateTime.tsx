import React, { FC } from 'react';
import { rankWith, ControlProps, isDateTimeControl } from '@jsonforms/core';
import { withJsonFormsControlProps } from '@jsonforms/react';
import {
  TextFieldProps,
  StandardTextFieldProps,
  BasicTextInput,
  DetailInputWithLabelRow,
  DateTimePicker,
  DateTimePickerProps,
  BaseDatePickerInput,
} from '@openmsupply-client/common';
import { FORM_LABEL_WIDTH } from '../styleConstants';
import { z } from 'zod';
import { useZodOptionsValidation } from '../hooks/useZodOptionsValidation';

const Options = z
  .object({
    /**
     *
     */
    dateOnly: z.boolean().optional(),
  })
  .strict()
  .optional();

type Options = z.infer<typeof Options>;

const DateTimePickerInput: FC<
  Omit<DateTimePickerProps<Date, Date>, 'renderInput'> & { error: string }
> = props => (
  <DateTimePicker
    disabled={props.disabled}
    renderInput={(params: TextFieldProps) => {
      const textInputProps: StandardTextFieldProps = {
        ...params,
        variant: 'standard',
      };
      return (
        <BasicTextInput
          error={!!props.error}
          helperText={props.error}
          FormHelperTextProps={
            !!props.error ? { sx: { color: 'error.main' } } : undefined
          }
          {...textInputProps}
        />
      );
    }}
    {...props}
  />
);

export const datetimeTester = rankWith(5, isDateTimeControl);

const UIComponent = (props: ControlProps) => {
  const [error, setError] = React.useState<string | undefined>(undefined);
  const { data, handleChange, label, path, uischema } = props;
  const { errors: zErrors, options } = useZodOptionsValidation(
    Options,
    uischema.options
  );

  if (!props.visible) {
    return null;
  }

  const dateOnly = options?.dateOnly ?? false;

  const inputFormat = !dateOnly ? 'dd/MM/yyyy hh:mm' : 'dd/MM/yyyy';

  const onChange = (e: Date | null) => {
    if (!e) return;

    try {
      setError(undefined);
      if (e) handleChange(path, e.toISOString());
    } catch (err) {
      setError((err as Error).message);
    }
  };

  const sharedComponentProps = {
    value: data ?? null,
    onChange: (e: Date | null) => onChange(e),
    inputFormat,
    readOnly: !!props.uischema.options?.['readonly'],
    disabled: !props.enabled,
    error: zErrors ?? error ?? props.errors,
  };

  return (
    <DetailInputWithLabelRow
      sx={{
        gap: 2,
        minWidth: '300px',
        justifyContent: 'space-around',
      }}
      label={label}
      labelWidthPercentage={FORM_LABEL_WIDTH}
      inputAlignment="start"
      Input={
        !dateOnly ? (
          <DateTimePickerInput
            // undefined is displayed as "now" and null as unset
            {...sharedComponentProps}
          />
        ) : (
          <BaseDatePickerInput {...sharedComponentProps} />
        )
      }
    />
  );
};

export const DateTime = withJsonFormsControlProps(UIComponent);