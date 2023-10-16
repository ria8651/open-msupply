import React, { FC } from 'react';
import { StandardTextFieldProps } from '@mui/material';
import { BasicTextInput } from '../BasicTextInput';
export interface NumericTextInputProps
  extends Omit<StandardTextFieldProps, 'onChange'> {
  onChange?: (value: number | undefined) => void;
  focusOnRender?: boolean;
  width?: number;
}

export const DEFAULT_NUMERIC_TEXT_INPUT_WIDTH = 75;

export const NumericTextInput: FC<NumericTextInputProps> = React.forwardRef(
  (
    {
      sx,
      InputProps,
      width = DEFAULT_NUMERIC_TEXT_INPUT_WIDTH,
      onChange,
      ...props
    },
    ref
  ) => (
    <BasicTextInput
      ref={ref}
      sx={{
        '& .MuiInput-input': { textAlign: 'right', width: `${width}px` },
        ...sx,
      }}
      InputProps={InputProps}
      onChange={e => {
        if (e.target.value === '' && !!onChange) {
          onChange(undefined);
          return;
        }
        const parsed = Number(e.target.value);
        if (!Number.isNaN(parsed) && !!onChange) onChange(parsed);
      }}
      onFocus={e => e.target.select()}
      type="number"
      {...props}
    />
  )
);
