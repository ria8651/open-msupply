import React, { FC, useState } from 'react';
import {
  ItemRowFragment,
  LocationRowFragment,
} from '@openmsupply-client/system';
import {
  BasicSpinner,
  Divider,
  useIsMediumScreen,
  Box,
  ModalMode,
  TableProvider,
  createTableStore,
  createQueryParamsStore,
  QueryParamsProvider,
  useRowHighlight,
  useNotification,
  useIsGrouped,
} from '@openmsupply-client/common';
import { StocktakeLineEditForm } from './StocktakeLineEditForm';
import { useStocktakeLineEdit } from './hooks';
import {
  StocktakeLineEditTabs,
  StyledTabContainer,
  StyledTabPanel,
  Tabs,
} from './StocktakeLineEditTabs';
import { useStocktake } from '../../../api';
import {
  LocationTable,
  BatchTable,
  PricingTable,
} from './StocktakeLineEditTables';
import { StocktakeLineEditModal } from './StocktakeLineEditModal';
interface StocktakeLineEditProps {
  item: ItemRowFragment | null;
  mode: ModalMode | null;
  onClose: () => void;
  isOpen: boolean;
}

export const StocktakeLineEdit: FC<StocktakeLineEditProps> = ({
  item,
  mode,
  onClose,
  isOpen,
}) => {
  const [currentItem, setCurrentItem] = useState(item);
  const isMediumScreen = useIsMediumScreen();
  const { isDisabled, items } = useStocktake.line.rows();
  const { draftLines, update, addLine, isSaving, save, nextItem } =
    useStocktakeLineEdit(currentItem);
  const { highlightRows } = useRowHighlight();
  const { error } = useNotification();
  const { isGrouped } = useIsGrouped('stocktake');

  // Order by newly added batch since new batches are now
  // added to the top of the stocktake list instead of the bottom
  const reversedDraftLines = [...draftLines].reverse();

  const onNext = async () => {
    if (isSaving) return;
    const { errorMessages } = await save();
    if (errorMessages) {
      errorMessages.forEach(errorMessage => error(errorMessage)());
      return;
    }

    if (mode === ModalMode.Update && nextItem) setCurrentItem(nextItem);
    else if (mode === ModalMode.Create) setCurrentItem(null);
    else onClose();
    // Returning true here triggers the slide animation
    return true;
  };

  const onOk = async () => {
    if (isSaving) return;
    const { errorMessages } = await save();
    if (errorMessages) {
      errorMessages.forEach(errorMessage => error(errorMessage)());
      return;
    }

    if (item) {
      const rowIds = draftLines.map(line =>
        isGrouped ? line.itemId : line.id
      );

      highlightRows({ rowIds });
    }
    onClose();
  };

  const hasValidBatches = draftLines.length > 0;

  return (
    <TableProvider
      createStore={createTableStore}
      queryParamsStore={createQueryParamsStore({
        initialSortBy: { key: 'expiryDate' },
      })}
    >
      <StocktakeLineEditModal
        onNext={onNext}
        onOk={onOk}
        onCancel={onClose}
        mode={mode}
        isOpen={isOpen}
        hasNext={!!nextItem}
        isValid={hasValidBatches && !isSaving}
      >
        {(() => {
          if (isSaving) {
            return (
              <Box sx={{ height: isMediumScreen ? 350 : 450 }}>
                <BasicSpinner messageKey="saving" />
              </Box>
            );
          }

          return (
            <>
              <StocktakeLineEditForm
                item={currentItem}
                items={items}
                onChangeItem={setCurrentItem}
                mode={mode}
              />
              {!currentItem ? (
                <Box sx={{ height: isMediumScreen ? 400 : 500 }} />
              ) : null}
              {!!currentItem ? (
                <>
                  <Divider margin={5} />
                  <StocktakeLineEditTabs
                    isDisabled={isDisabled}
                    onAddLine={addLine}
                  >
                    <StyledTabPanel value={Tabs.Batch}>
                      <StyledTabContainer>
                        <BatchTable
                          isDisabled={isDisabled}
                          batches={reversedDraftLines}
                          update={update}
                        />
                      </StyledTabContainer>
                    </StyledTabPanel>

                    <StyledTabPanel value={Tabs.Pricing}>
                      <StyledTabContainer>
                        <PricingTable
                          isDisabled={isDisabled}
                          batches={reversedDraftLines}
                          update={update}
                        />
                      </StyledTabContainer>
                    </StyledTabPanel>

                    <StyledTabPanel value={Tabs.Location}>
                      <StyledTabContainer>
                        <QueryParamsProvider
                          createStore={createQueryParamsStore<LocationRowFragment>(
                            {
                              initialSortBy: { key: 'name' },
                            }
                          )}
                        >
                          <LocationTable
                            isDisabled={isDisabled}
                            batches={reversedDraftLines}
                            update={update}
                          />
                        </QueryParamsProvider>
                      </StyledTabContainer>
                    </StyledTabPanel>
                  </StocktakeLineEditTabs>
                </>
              ) : null}
            </>
          );
        })()}
      </StocktakeLineEditModal>
    </TableProvider>
  );
};
