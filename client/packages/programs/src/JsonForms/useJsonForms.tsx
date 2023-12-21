import React, { useEffect, useState } from 'react';
import {
  useTranslation,
  useNotification,
  useConfirmOnLeaving,
} from '@openmsupply-client/common';
import { JsonData, JsonForm, JsonFormsConfig } from './common';
import _ from 'lodash';
import {
  JsonFormsRendererRegistryEntry,
  JsonSchema,
  UISchemaElement,
} from '@jsonforms/core';
import {
  EncounterLineChart,
  encounterLineChartTester,
  BMI,
  bmiTester,
  DateOfBirth,
  dateOfBirthTester,
  IdGenerator,
  idGeneratorTester,
  QuantityPrescribed,
  quantityDispensedTester,
  AdherenceScore,
  adherenceScoreTester,
  PreviousEncounterField,
  previousEncounterFieldTester,
  DecisionTreeControl,
  decisionTreeTester,
  Search,
  searchTester,
  programEventTester,
  ProgramEvent,
  historicEncounterDataTester,
  HistoricEncounterData,
  bloodPressureTester,
  BloodPressure,
} from './components';
import { EnrolmentId, enrolmentIdTester } from './components/EnrolmentId';

// https://stackoverflow.com/questions/57874879/how-to-treat-missing-undefined-properties-as-equivalent-in-lodashs-isequalwit
// TODO: handle undefined and empty string as equal? e.g. initial data is undefined and current data is ""
const isEqualIgnoreUndefined = (
  a: JsonData | undefined,
  b: JsonData | undefined
) => {
  const comparisonFunc = (a: JsonData | undefined, b: JsonData | undefined) => {
    if (_.isArray(a) || _.isArray(b)) return;
    if (!_.isObject(a) || !_.isObject(b)) return;

    if (!_.includes(a, undefined) && !_.includes(b, undefined)) return;

    // Call recursively, after filtering all undefined properties
    return _.isEqualWith(
      _.omitBy(a, value => value === undefined),
      _.omitBy(b, value => value === undefined),
      comparisonFunc
    );
  };
  return _.isEqualWith(a, b, comparisonFunc);
};

export interface SchemaData {
  formSchemaId?: string;
  jsonSchema: JsonSchema;
  uiSchema: UISchemaElement;
}

/**
 * Information required to create a new document
 */
export interface FormInputData {
  data: JsonData;
  schema: SchemaData;
  /**
   * Indicates if data is newly created, i.e. if the data isDirty.
   * For example, if a patient data may come from the NameRow instead of a document and thus is
   * can be marked with `isCreating: false`.
   */
  isCreating: boolean;
}

const additionalRenderers: JsonFormsRendererRegistryEntry[] = [
  { tester: idGeneratorTester, renderer: IdGenerator },
  { tester: dateOfBirthTester, renderer: DateOfBirth },
  { tester: encounterLineChartTester, renderer: EncounterLineChart },
  { tester: quantityDispensedTester, renderer: QuantityPrescribed },
  { tester: bmiTester, renderer: BMI },
  { tester: adherenceScoreTester, renderer: AdherenceScore },
  {
    tester: previousEncounterFieldTester,
    renderer: PreviousEncounterField,
  },
  { tester: decisionTreeTester, renderer: DecisionTreeControl },
  { tester: searchTester, renderer: Search },
  { tester: programEventTester, renderer: ProgramEvent },
  { tester: historicEncounterDataTester, renderer: HistoricEncounterData },
  { tester: enrolmentIdTester, renderer: EnrolmentId },
  { tester: bloodPressureTester, renderer: BloodPressure },
];

/**
 * Data input for the useJsonForms hook.
 *
 * The data and save method can be provided by an external hook.
 */
export type JsonFormData<R> = {
  /** Initial form data */
  loadedData: JsonData;
  /** Initial form data is still loading */
  isLoading: boolean;
  /** There was an error loading the initial form data */
  error: string | undefined;
  /** Indicates if the initial form data is going be created (is dirty) */
  isCreating: boolean;
  /** The schema of the data */
  schema: SchemaData | undefined;
  /** Method to update the form data */
  save?: (data: unknown) => Promise<R>;
};

/**
 * This hook add provides functionality to save form data and keep track if form data has been
 * modified.
 *
 * What data is shown and how it is saved can be customized through the `jsonFormData` form
 * parameter.
 */
export const useJsonForms = <R,>(
  config: JsonFormsConfig,
  jsonFormData: JsonFormData<R>
) => {
  const { loadedData, isLoading, error, save, isCreating } = jsonFormData;
  const [initialData, setInitialData] = useState<JsonData | undefined>(
    loadedData
  );
  useEffect(() => {
    setInitialData(loadedData);
  }, [loadedData]);
  // current modified data
  const [data, setData] = useState<JsonData | undefined>();
  const [isSaving, setSaving] = useState(false);
  const [isDirty, setIsDirty] = useState<boolean>();
  const t = useTranslation('common');
  const [validationError, setValidationError] = useState<string | false>(false);
  const { success, error: errorNotification } = useNotification();

  useConfirmOnLeaving(isDirty);

  // returns the document name
  const saveData = async (deletion?: boolean): Promise<R | undefined> => {
    if (data === undefined) {
      return undefined;
    }
    setSaving(true);

    // Run mutation...
    try {
      const result = await save?.(data);

      const successSnack = success(
        deletion ? t('success.data-deleted') : t('success.data-saved')
      );
      successSnack();

      setInitialData(data);
      return result;
    } catch (err) {
      const errorSnack = errorNotification(t('error.problem-saving'));
      errorSnack();
    } finally {
      setSaving(false);
    }
  };

  const revert = () => {
    setIsDirty(false);
    setData(initialData);
  };

  const updateData = (newData: JsonData) => {
    setData(newData);
  };

  useEffect(() => {
    const dirty =
      isSaving ||
      isLoading ||
      // document doesn't exist yet; always set the isDirty flag
      isCreating ||
      !isEqualIgnoreUndefined(initialData, data);
    setIsDirty(dirty);
    if (data === undefined) {
      setData(initialData);
    }
  }, [initialData, data, isSaving, isLoading, isCreating]);

  useEffect(() => {
    setData(initialData);
    return () => setIsDirty(false);
  }, [initialData]);

  const schema = jsonFormData.schema;
  return {
    JsonForm: (
      <JsonForm
        data={data}
        jsonSchema={schema?.jsonSchema ?? {}}
        uiSchema={schema?.uiSchema ?? { type: 'Control' }}
        isError={!!error}
        isLoading={isLoading}
        setError={setValidationError}
        updateData={updateData}
        additionalRenderers={additionalRenderers}
        config={{
          ...config,
          initialData,
        }}
      />
    ),
    data,
    setData,
    saveData,
    revert,
    isSaving,
    isLoading,
    isDirty: isDirty ?? false,
    error,
    validationError,
  };
};
