import React, { useState } from 'react';
import {
  BasicSpinner,
  useNotification,
  Box,
  useDialog,
  useTranslation,
  DialogButton,
  InputWithLabelRow,
  Select,
  Autocomplete,
  FnUtils,
  InsertAssetInput,
  BasicTextInput,
} from '@openmsupply-client/common';
import {
  AssetCatalogueItemFragment,
  mapIdNameToOptions,
  useAssetData,
} from '@openmsupply-client/system';
import { useAssets } from '../api';

interface CreateAssetModalProps {
  isOpen: boolean;
  onClose: () => void;
}

const mapCatalogueItem = (catalogueItem: AssetCatalogueItemFragment) => ({
  label: `${catalogueItem.code} Type Manufacturer Model`,
  value: catalogueItem.id,
});

const mapCatalogueItems = (catalogueItems: AssetCatalogueItemFragment[]) =>
  catalogueItems.map(mapCatalogueItem);

const EmptyAssetInput = {
  id: FnUtils.generateUUID(),
  name: '',
  code: '',
  catalogueItemId: '',
};

const InputRow = ({
  label,
  Input,
}: {
  label: string;
  Input: React.ReactNode;
}) => (
  <InputWithLabelRow
    labelWidth="150"
    sx={{ marginTop: 2 }}
    label={label}
    Input={Input}
  />
);

export const CreateAssetModal = ({
  isOpen,
  onClose,
}: CreateAssetModalProps) => {
  const t = useTranslation('coldchain');
  const { error, success } = useNotification();
  const { Modal } = useDialog({ isOpen, onClose });
  const [categoryId, setCategoryId] = useState('');
  const [draft, setDraft] = useState<InsertAssetInput>({ ...EmptyAssetInput });
  const { data: categoryData, isLoading: isLoadingCategories } =
    useAssetData.utils.categories();
  const { data: catalogueItemData } = useAssetData.document.list(categoryId);
  const { mutateAsync: save } = useAssets.document.insert();

  const handleClose = () => {
    setCategoryId('');
    setDraft({ ...EmptyAssetInput });
    onClose();
  };

  const updateDraft = (patch: Partial<InsertAssetInput>) => {
    setDraft({ ...draft, ...patch });
  };

  const catalogueItems = catalogueItemData?.nodes ?? [];
  const selectedCatalogueItem = catalogueItems.find(
    ci => ci.id === draft.catalogueItemId
  );
  return (
    <Modal
      title={t('heading.add-cold-chain-equipment')}
      width={700}
      height={100}
      cancelButton={<DialogButton variant="cancel" onClick={handleClose} />}
      okButton={
        <DialogButton
          variant="ok"
          disabled={!draft.catalogueItemId || !draft.name || !draft.code}
          onClick={async () => {
            try {
              await save(draft);
              success(t('message.cce-created'))();
              handleClose();
            } catch (e) {
              error(t('error.unable-to-create-cce'))();
            }
          }}
        />
      }
    >
      {isLoadingCategories ? (
        <BasicSpinner messageKey="loading" />
      ) : (
        <Box>
          <InputRow
            label={t('label.category')}
            Input={
              <Select
                options={mapIdNameToOptions(categoryData?.nodes ?? [])}
                fullWidth
                onChange={e => setCategoryId(e.target.value)}
              />
            }
          />
          <InputRow
            label={t('label.catalogue-item')}
            Input={
              <Autocomplete
                value={
                  !!selectedCatalogueItem
                    ? mapCatalogueItem(selectedCatalogueItem)
                    : undefined
                }
                options={mapCatalogueItems(catalogueItems)}
                width="100%"
                sx={{ width: '100%' }}
                onChange={(_event, selected) =>
                  updateDraft({ catalogueItemId: selected?.value ?? '' })
                }
              />
            }
          />
          <InputRow
            label={t('label.code')}
            Input={
              <BasicTextInput
                fullWidth
                value={draft.code}
                onChange={e => updateDraft({ code: e.target.value })}
              />
            }
          />
          <InputRow
            label={t('label.name')}
            Input={
              <BasicTextInput
                fullWidth
                value={draft.name}
                onChange={e => updateDraft({ name: e.target.value })}
              />
            }
          />
        </Box>
      )}
    </Modal>
  );
};
