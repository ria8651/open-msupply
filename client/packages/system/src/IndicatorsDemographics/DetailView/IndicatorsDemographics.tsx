import React, { FC, useEffect, useState } from 'react';
import { AppBarButtons } from './AppBarButtons';
import {
  Box,
  ColumnAlign,
  DataTable,
  RecordPatch,
  TableProvider,
  createTableStore,
  useColumns,
  useNotification,
  useTranslation,
  useUrlQueryParams,
} from '@openmsupply-client/common';

import { percentageColumn } from './PercentageColumn';
import { nameColumn } from './NameColumn';
import { GrowthRow } from './GrowthRow';
import { populationColumn } from './PopulationColumn';
import { Footer } from './Footer';
import { GENERAL_POPULATION_ID, useDemographicData } from '../api';
import {
  calculateAcrossRow,
  getDefaultHeaderData,
  mapHeaderData,
  mapProjection,
  toIndicatorFragment,
} from './utils';
import { HeaderData, HeaderValue, Row } from '../types';

const currentYear = new Date().getFullYear();

const IndicatorsDemographicsComponent: FC = () => {
  const {
    updateSortQuery,
    queryParams: { sortBy },
  } = useUrlQueryParams({
    initialSort: { key: 'percentage', dir: 'desc' },
  });
  const [headerDraft, setHeaderDraft] = useState<HeaderData>();
  const [indexPopulation, setIndexPopulation] = useState(0);
  const [isDirty, setIsDirty] = useState(false);

  const { error, success } = useNotification();
  const t = useTranslation();

  const { draft, setDraft } = useDemographicData.indicator.list(headerDraft);
  const { data: projections, isLoading: isLoadingProjections } =
    useDemographicData.projection.list(draft?.[0]?.baseYear ?? 2024);

  const { insertDemographicIndicator, invalidateQueries } =
    useDemographicData.indicator.insert();
  const { mutateAsync: updateDemographicIndicator } =
    useDemographicData.indicator.update();
  const upsertProjection = useDemographicData.projection.upsert();
  const getHeaderDraft = () =>
    headerDraft ?? getDefaultHeaderData(draft?.[0]?.baseYear ?? 0);

  const PopulationChange = (patch: RecordPatch<Row>) => {
    setIsDirty(true);
    const currentDraft = { ...draft, [patch.id]: patch } as Record<string, Row>;
    let updatedDraft = {} as Record<string, Row>;
    const indexPopulationChange =
      patch.basePopulation !== draft[patch.id]?.basePopulation &&
      patch.id === GENERAL_POPULATION_ID;

    if (indexPopulationChange) setIndexPopulation(Number(patch.basePopulation));

    Object.keys(currentDraft).forEach(rowKey => {
      const updatedRow = calculateAcrossRow(
        currentDraft[rowKey] as Row,
        getHeaderDraft(),
        indexPopulationChange ? patch.basePopulation : indexPopulation
      );
      updatedDraft = { ...updatedDraft, [updatedRow.id]: updatedRow };
    });
    setDraft({ ...currentDraft, ...updatedDraft });
  };

  const setter = (patch: RecordPatch<Row>) => {
    const updatedDraft = { ...draft };
    const percentage = !patch.percentage ? 0 : patch.percentage;
    const percentageChange = percentage != draft[patch.id]?.percentage;

    setIsDirty(true);

    // change state of name only if only name changes
    if (!percentageChange) {
      setDraft({ ...updatedDraft, [patch.id]: { ...patch } as Row });
      return;
    }

    const updatedRow = calculateAcrossRow(
      { ...patch } as Row,
      getHeaderDraft(),
      indexPopulation
    );
    setDraft({ ...updatedDraft, [patch.id]: updatedRow });
  };

  // generic function for handling percentage change, and then re calculating the values of that year
  const handleGrowthChange = (patch: RecordPatch<HeaderValue>) => {
    setIsDirty(true);
    const updatedPatch = {
      ...patch,
      value: patch.value ?? 0,
    };
    setHeaderDraft({ ...getHeaderDraft(), [patch.id]: updatedPatch });
    calculateDown(patch);
  };
  const calculateDown = (patch: RecordPatch<HeaderValue>) => {
    const updatedHeader = {
      ...getHeaderDraft(),
      [patch.id]: { ...patch } as HeaderValue,
    };
    const updatedDraft: Record<string, Row> = {};
    Object.keys(draft).forEach(row => {
      const updatedRow = calculateAcrossRow(
        draft[row] as Row,
        updatedHeader,
        indexPopulation
      );
      updatedDraft[updatedRow.id] = updatedRow;
    });
    setHeaderDraft(updatedHeader);
    setDraft(updatedDraft);
  };

  const insertIndicator = async (row: Row) => {
    try {
      await insertDemographicIndicator(
        toIndicatorFragment(row, indexPopulation)
      );
    } catch (e) {
      console.error(e);
    }
  };

  const updateIndicator = async (row: Row) => {
    try {
      await updateDemographicIndicator(
        toIndicatorFragment(row, indexPopulation)
      );
    } catch (e) {
      console.error(e);
    }
  };

  // save rows excluding generalRow
  const save = async () => {
    setIsDirty(false);
    const remainingRows = Object.keys(draft)
      .map(key => draft[key])
      .filter(row => row?.id !== GENERAL_POPULATION_ID);

    while (remainingRows.length) {
      await Promise.all(
        remainingRows.splice(0).map(async indicator => {
          if (indicator != undefined) {
            indicator.isNew
              ? await insertIndicator(indicator)
              : await updateIndicator(indicator);
          }
        })
      )
        .then(async () => {
          if (headerDraft !== undefined)
            await upsertProjection(mapProjection(headerDraft));
        })
        .then(() => success(t('success.data-saved'))())
        .then(() => invalidateQueries())
        .catch(e => error(`${t('error.problem-saving')}: ${e.message}`)());
    }
  };

  const cancel = () => {
    window.location.reload();
  };

  const columns = useColumns<Row>(
    [
      [percentageColumn(), { setter }],
      [nameColumn(), { setter }],
      [populationColumn(), { setter: PopulationChange }],
      {
        key: '1',
        width: 150,
        align: ColumnAlign.Right,
        label: undefined,
        labelProps: { defaultValue: currentYear + 1 },
      },
      {
        key: '2',
        width: 150,
        align: ColumnAlign.Right,
        label: undefined,
        labelProps: { defaultValue: currentYear + 2 },
      },
      {
        key: '3',
        width: 150,
        align: ColumnAlign.Right,
        label: undefined,
        labelProps: { defaultValue: currentYear + 3 },
      },
      {
        key: '4',
        width: 150,
        align: ColumnAlign.Right,
        label: undefined,
        labelProps: { defaultValue: currentYear + 4 },
      },
    ],
    { sortBy, onChangeSortBy: updateSortQuery },
    [draft, indexPopulation]
  );

  useEffect(() => {
    if (!draft || !!indexPopulation) return;

    const generalPopulationRow = draft[GENERAL_POPULATION_ID];
    if (!generalPopulationRow) return;

    setIndexPopulation(generalPopulationRow.basePopulation);

    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [draft]);

  useEffect(() => {
    if (!projections || !projections.nodes[0]) return;

    setHeaderDraft(mapHeaderData(projections.nodes[0]));
  }, [projections]);

  return (
    <>
      <AppBarButtons patch={setter} rows={Object.values(draft)}></AppBarButtons>
      <Box>
        <GrowthRow
          columns={columns}
          data={headerDraft}
          isLoading={isLoadingProjections}
          setData={handleGrowthChange}
        ></GrowthRow>
        <DataTable
          data={Object.values(draft)}
          columns={columns}
          id={'indicators-demographics-table'}
        ></DataTable>
      </Box>
      <Footer save={save} cancel={cancel} isDirty={isDirty} />
    </>
  );
};

export const IndicatorsDemographics: FC = () => (
  <TableProvider createStore={createTableStore}>
    <IndicatorsDemographicsComponent />
  </TableProvider>
);
