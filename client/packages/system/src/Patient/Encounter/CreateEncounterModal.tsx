import React, { FC, useState } from 'react';
import {
  AlertIcon,
  BasicSpinner,
  Box,
  DialogButton,
  EncounterNodeStatus,
  InputWithLabelRow,
  RouteBuilder,
  Stack,
  Typography,
  useDialog,
  useNavigate,
  useNotification,
  useAuthContext,
  DatePickerInput,
  TextArea,
} from '@openmsupply-client/common';
import { DateUtils, useIntlUtils, useTranslation } from '@common/intl';
import {
  EncounterRegistryByProgram,
  PatientModal,
  usePatientModalStore,
  useEncounter,
  NoteSchema,
} from '@openmsupply-client/programs';
import { usePatient } from '../api';
import { AppRoute } from '@openmsupply-client/config';
import { EncounterSearchInput } from './EncounterSearchInput';
import {
  Clinician,
  ClinicianAutocompleteOption,
  ClinicianSearchInput,
} from '../../Clinician';

interface Encounter {
  status?: EncounterNodeStatus;
  createdDatetime: string;
  createdBy?: { id: string; username: string };
  startDatetime?: string;
  endDatetime?: string;
  clinician?: Clinician;
  location?: { storeId?: string };
  notes?: NoteSchema[];
}

export const CreateEncounterModal: FC = () => {
  const patientId = usePatient.utils.id();
  const { user, storeId } = useAuthContext();
  const t = useTranslation('patients');
  const { getLocalisedFullName } = useIntlUtils();
  const { current, setModal: selectModal } = usePatientModalStore();
  const [encounterRegistry, setEncounterRegistry] = useState<
    EncounterRegistryByProgram | undefined
  >();
  const [createdDatetime] = useState(new Date().toISOString());
  const [dataError, setDataError] = useState(false);
  const [draft, setDraft] = useState<Encounter | undefined>(undefined);
  const navigate = useNavigate();
  const { error } = useNotification();
  const [startDateTimeError, setStartDateTimeError] = useState(false);
  const [note] = useState<NoteSchema | undefined>(undefined);

  const handleSave = useEncounter.document.upsert(
    patientId,
    encounterRegistry?.encounter.documentType ?? ''
  );

  const reset = () => {
    selectModal(undefined);
    setEncounterRegistry(undefined);
    setDraft(undefined);
    setDataError(false);
    setNote(undefined);
  };

  const { Modal } = useDialog({
    isOpen: current === PatientModal.Encounter,
    onClose: reset,
  });

  const onChangeEncounter = (entry: EncounterRegistryByProgram) => {
    setDataError(false);
    setEncounterRegistry(entry);
  };

  const currentOrNewDraft = (): Encounter => {
    return (
      draft ?? {
        createdDatetime,
        createdBy: { id: user?.id ?? '', username: user?.name ?? '' },
        status: EncounterNodeStatus.Pending,
        location: {
          storeId,
        },
      }
    );
  };
  const setStartDatetime = (date: Date | null): void => {
    const startDatetime = DateUtils.formatRFC3339(
      DateUtils.addCurrentTime(date)
    );
    setDraft({
      ...currentOrNewDraft(),
      startDatetime,
    });
    setStartDateTimeError(false);
  };

  const setClinician = (option: ClinicianAutocompleteOption | null): void => {
    if (option === null) {
      setDraft({ ...currentOrNewDraft(), clinician: undefined });
      return;
    }
    const clinician = option.value;
    setDraft({ ...currentOrNewDraft(), clinician });
  };

  const setNote = (notes: NoteSchema[] | undefined): void => {
    setDraft({ ...currentOrNewDraft(), notes });
  };

  const canSubmit = () =>
    draft !== undefined && draft.startDatetime && !startDateTimeError;

  return (
    <Modal
      title={t('label.new-encounter')}
      cancelButton={<DialogButton variant="cancel" onClick={reset} />}
      okButton={
        <DialogButton
          variant={'save'}
          disabled={!canSubmit()}
          onClick={async () => {
            if (encounterRegistry !== undefined) {
              const { id } = await handleSave(
                draft,
                encounterRegistry.encounter.formSchemaId
              );
              if (!!id)
                navigate(
                  RouteBuilder.create(AppRoute.Dispensary)
                    .addPart(AppRoute.Encounter)
                    .addPart(id)
                    .build()
                );
              else error(t('error.encounter-not-created'))();
            }
            reset();
          }}
        />
      }
      width={700}
    >
      <React.Suspense fallback={<div />}>
        <Stack alignItems="flex-start" gap={1} sx={{ paddingLeft: '20px' }}>
          <InputWithLabelRow
            label={t('label.encounter')}
            Input={
              <EncounterSearchInput
                onChange={onChangeEncounter}
                value={null}
                width={250}
              />
            }
          />
          <RenderForm
            isError={dataError}
            isLoading={false}
            isProgram={!!encounterRegistry}
            form={
              <>
                <InputWithLabelRow
                  label={t('label.visit-date')}
                  Input={
                    <DatePickerInput
                      value={draft?.startDatetime ?? null}
                      onChange={setStartDatetime}
                      onError={() => setStartDateTimeError(true)}
                      width={250}
                    />
                  }
                />
                <InputWithLabelRow
                  label={t('label.clinician')}
                  Input={
                    <ClinicianSearchInput
                      onChange={setClinician}
                      clinicianLabel={getLocalisedFullName(
                        draft?.clinician?.firstName,
                        draft?.clinician?.lastName
                      )}
                      clinicianValue={draft?.clinician}
                      width={250}
                    />
                  }
                />
                <InputWithLabelRow
                  label={t('label.visit-notes')}
                  Input={
                    <TextArea
                      InputProps={{
                        sx: {
                          backgroundColor: 'background.drawer',
                        },
                      }}
                      value={note}
                      onChange={e => {
                        setNote([
                          {
                            authorId: user?.id,
                            authorName: user?.name,
                            created: new Date().toISOString(),
                            text: e.target.value,
                          },
                        ]);
                      }}
                    />
                  }
                />
              </>
            }
          />
        </Stack>
      </React.Suspense>
    </Modal>
  );
};

const RenderForm = ({
  form,
  isError,
  isLoading,
  isProgram,
}: {
  form: React.ReactNode;
  isError: boolean;
  isLoading: boolean;
  isProgram: boolean;
}) => {
  const t = useTranslation('common');
  if (!isProgram) return null;
  if (isError)
    return (
      <Box display="flex" gap={1} padding={3}>
        <AlertIcon color="error" />
        <Typography color="error">{t('error.unable-to-load-data')}</Typography>
      </Box>
    );
  if (isLoading) return <BasicSpinner />;

  return <>{form}</>;
};