import React, { useEffect, useRef, useState } from 'react';
import {
  useTranslation,
  useDialog,
  DialogButton,
  TableProvider,
  createTableStore,
  useKeyboardHeightAdjustment,
  useTabs,
  ModalMode,
  Box,
  AlertColor,
} from '@openmsupply-client/common';
import { useDraftInboundReturnLines } from './useDraftInboundReturnLines';
import { ItemSelector } from './ItemSelector';
import { ReturnSteps, Tabs } from './ReturnSteps';

interface InboundReturnEditModalProps {
  isOpen: boolean;
  outboundShipmentLineIds: string[];
  customerId: string;
  onClose: () => void;
  modalMode: ModalMode | null;
  returnId?: string;
  initialItemId?: string | null;
  loadNextItem?: () => void;
}

export const InboundReturnEditModal = ({
  isOpen,
  outboundShipmentLineIds,
  customerId,
  onClose,
  modalMode,
  returnId,
  initialItemId,
  loadNextItem,
}: InboundReturnEditModalProps) => {
  const t = useTranslation('distribution');
  const { currentTab, onChangeTab } = useTabs(Tabs.Quantity);

  const [itemId, setItemId] = useState<string | undefined>(
    initialItemId ?? undefined
  );

  const alertRef = useRef<HTMLDivElement>(null);

  const [zeroQuantityAlert, setZeroQuantityAlert] = useState<
    AlertColor | undefined
  >();

  const { Modal } = useDialog({ isOpen, onClose, disableBackdrop: true });
  const height = useKeyboardHeightAdjustment(700);

  const { lines, update, save, addDraftLine } = useDraftInboundReturnLines({
    outboundShipmentLineIds,
    customerId,
    returnId,
    itemId,
  });

  useEffect(() => {
    if (initialItemId) setItemId(initialItemId);
  }, [initialItemId]);

  const onOk = async () => {
    try {
      await save();
      onClose();
    } catch {
      // TODO: handle error display...
    }
  };

  const handleNextItem = async () => {
    try {
      await save();
      loadNextItem && loadNextItem();
      onChangeTab(Tabs.Quantity);
    } catch {
      // TODO: handle error display...
    }
  };

  const handleNextStep = () => {
    if (lines.some(line => line.numberOfPacksReturned !== 0)) {
      onChangeTab(Tabs.Reason);
      return;
    }
    switch (modalMode) {
      case ModalMode.Create: {
        setZeroQuantityAlert('error');
        break;
      }
      case ModalMode.Update: {
        setZeroQuantityAlert('warning');
        break;
      }
    }
    alertRef?.current?.scrollIntoView({ behavior: 'smooth' });
  };

  const CancelButton = <DialogButton onClick={onClose} variant="cancel" />;
  const BackButton = (
    <DialogButton onClick={() => onChangeTab(Tabs.Quantity)} variant="back" />
  );
  const NextStepButton = (
    <DialogButton
      onClick={handleNextStep}
      variant="next"
      disabled={!lines.length}
      customLabel={t('button.next-step')}
    />
  );
  const OkButton = <DialogButton onClick={onOk} variant="ok" />;
  const OkAndNextButton = (
    <DialogButton
      onClick={handleNextItem}
      variant="next"
      disabled={currentTab !== Tabs.Reason}
    />
  );

  return (
    <TableProvider createStore={createTableStore}>
      <Modal
        title={t('heading.return-items')}
        cancelButton={currentTab === Tabs.Quantity ? CancelButton : BackButton}
        // zeroQuantityAlert === warning implies all lines are 0 and user has
        // been already warned, so we act immediately to update them
        okButton={
          currentTab === Tabs.Quantity && zeroQuantityAlert !== 'warning'
            ? NextStepButton
            : OkButton
        }
        nextButton={
          modalMode === ModalMode.Update && loadNextItem
            ? OkAndNextButton
            : undefined
        }
        height={height}
        width={1024}
      >
        <Box ref={alertRef}>
          {returnId && (
            <ItemSelector
              disabled={!!itemId}
              itemId={itemId}
              onChangeItemId={setItemId}
            />
          )}

          {lines.length > 0 && (
            <ReturnSteps
              currentTab={currentTab}
              lines={lines}
              update={update}
              zeroQuantityAlert={zeroQuantityAlert}
              setZeroQuantityAlert={setZeroQuantityAlert}
              // We only allow adding draft lines when we are adding by item
              addDraftLine={itemId ? addDraftLine : undefined}
            />
          )}
        </Box>
      </Modal>
    </TableProvider>
  );
};
