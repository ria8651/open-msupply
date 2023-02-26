import React, { useEffect, useMemo, useState } from 'react';
import { ControlProps, rankWith, schemaTypeIs } from '@jsonforms/core';
import { withJsonFormsControlProps } from '@jsonforms/react';
import {
  DetailInputWithLabelRow,
  useTranslation,
} from '@openmsupply-client/common';
import { z } from 'zod';
import { useZodOptionsValidation } from '../hooks/useZodOptionsValidation';
import { useDebouncedTextInput } from '../hooks/useDebouncedTextInput';
import { FORM_LABEL_WIDTH } from '../styleConstants';
import { useJSONFormsCustomError } from '../hooks/useJSONFormsCustomError';
import { DefaultFormRowSx } from '../../style';

const Options = z
  .object({
    /**
     * Additional pattern to be matched that can be defined in ui schema
     */
    pattern: z.string().optional(),
    /**
     * Examples for the correct pattern
     */
    examples: z.array(z.string()).optional(),
    width: z.string().optional(),
    /**
     * If true, text input will expand to multiple lines if required (default:
     * true)
     */
    multiline: z.boolean().optional(),
    /**
     * How many rows should the textbox display initially (default: 1, ignored
     * if `multiline === false`)
     */
    rows: z.number().optional(),
  })
  .strict()
  .optional();

type Options = z.infer<typeof Options>;

// Validates the option and parses the pattern into RegEx
const useOptions = (
  options?: Record<string, unknown>
): { errors?: string; options?: Options; pattern?: RegExp } => {
  const [regexError, setRegexErrors] = useState<string | undefined>();
  const { errors: zErrors, options: schemaOptions } = useZodOptionsValidation(
    Options,
    options
  );
  const pattern = useMemo(() => {
    if (!schemaOptions?.pattern) {
      return undefined;
    }
    try {
      return new RegExp(schemaOptions?.pattern);
    } catch {
      setRegexErrors(`Invalid regex: ${schemaOptions.pattern}`);
      return undefined;
    }
  }, [schemaOptions?.pattern]);

  return { errors: zErrors ?? regexError, options: schemaOptions, pattern };
};

// Returns error if value doesn't match the pattern
const usePatternValidation = (
  path: string,
  pattern?: RegExp,
  value?: string
): string | undefined => {
  const { customError, setCustomError } = useJSONFormsCustomError(path, 'Text');

  useEffect(() => {
    if (!pattern || !value) {
      setCustomError(undefined);
      return;
    }
    const result = pattern.exec(value);
    if (result == null) {
      setCustomError('Invalid format');
    } else {
      setCustomError(undefined);
    }
  }, [pattern, value]);
  return customError;
};

export const stringTester = rankWith(3, schemaTypeIs('string'));

const UIComponent = (props: ControlProps) => {
  const { data, path, handleChange, errors, label } = props;
  const {
    errors: zErrors,
    options: schemaOptions,
    pattern,
  } = useOptions(props.uischema.options);
  const customErrors = usePatternValidation(path, pattern, data);
  const error = !!errors || !!zErrors || !!customErrors;
  const { text, onChange } = useDebouncedTextInput(
    data,
    (value: string | undefined) => handleChange(path, value)
  );
  const t = useTranslation('common');

  const examples =
    (props.schema as Record<string, string[]>)['examples'] ??
    schemaOptions?.examples;
  const helperText =
    (!!customErrors || !!errors) && examples && Array.isArray(examples)
      ? t('error.json-bad-format-with-examples', {
          examples: examples.join('", "'),
        })
      : zErrors ?? errors ?? customErrors;

  if (!props.visible) {
    return null;
  }

  const multiline = schemaOptions?.multiline !== false;
  const rows = schemaOptions?.rows;

  const width = schemaOptions?.width ?? '100%';

  return (
    <DetailInputWithLabelRow
      sx={DefaultFormRowSx}
      label={label}
      inputProps={{
        value: text ?? '',
        sx: { width },
        onChange: e => onChange(e.target.value || ''),
        disabled: !props.enabled,
        error,
        helperText,
        FormHelperTextProps: error
          ? { sx: { color: 'error.main' } }
          : undefined,
        required: props.required,
        multiline,
        rows,
      }}
      labelWidthPercentage={FORM_LABEL_WIDTH}
      inputAlignment={'start'}
    />
  );
};

export const TextField = withJsonFormsControlProps(UIComponent);
