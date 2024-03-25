import React, { FC } from 'react';
import {
  TableProvider,
  createTableStore,
  DetailViewSkeleton,
  AlertModal,
  useNavigate,
  RouteBuilder,
  useTranslation,
  createQueryParamsStore,
  DetailTabs,
  useEditModal,
} from '@openmsupply-client/common';
import { ContentArea } from './ContentArea';
import { Toolbar } from './Toolbar';
import { AppBarButtons } from './AppBarButtons';
import { InboundReturnLineFragment, useReturns } from '../api';
import { AppRoute } from '@openmsupply-client/config';
import { SidePanel } from './SidePanel/SidePanel';
import { ActivityLogList } from '@openmsupply-client/system';
import { Footer } from './Footer';
import { InboundReturnItem } from '../../types';
import { InboundReturnEditModal } from '../modals';

export const InboundReturnDetailView: FC = () => {
  const { data, isLoading } = useReturns.document.inboundReturn();
  const t = useTranslation('distribution');
  const navigate = useNavigate();

  const {
    onOpen,
    onClose,
    isOpen,
    entity: itemId,
    mode,
  } = useEditModal<string>();

  const onRowClick = (row: InboundReturnLineFragment | InboundReturnItem) =>
    onOpen(row.itemId);

  if (isLoading) return <DetailViewSkeleton hasGroupBy={true} hasHold={true} />;

  const tabs = [
    {
      Component: <ContentArea onRowClick={onRowClick} onAddItem={onOpen} />,
      value: t('label.details'),
    },
    {
      Component: <ActivityLogList recordId={data?.id ?? ''} />,
      value: t('label.log'),
    },
  ];

  const getNextItemId = () => {
    const lines = data?.lines?.nodes;
    if (!lines) return undefined;
    const currentItemIndex = lines.findIndex(line => line.itemId === itemId);
    if (currentItemIndex === -1) return;

    const nextItemIndex = lines.findIndex(
      (line, index) => index > currentItemIndex && line.itemId !== itemId
    );
    return nextItemIndex === -1 ? undefined : lines[nextItemIndex]?.itemId;
  };

  const nextItemId = getNextItemId();

  return (
    <React.Suspense
      fallback={<DetailViewSkeleton hasGroupBy={true} hasHold={true} />}
    >
      {data ? (
        <TableProvider
          createStore={createTableStore}
          queryParamsStore={createQueryParamsStore<InboundReturnLineFragment>({
            initialSortBy: {
              key: 'itemName',
            },
          })}
        >
          <AppBarButtons onAddItem={onOpen} />
          {isOpen && (
            <InboundReturnEditModal
              isOpen={isOpen}
              onClose={onClose}
              outboundShipmentLineIds={[]}
              customerId={data.otherPartyId}
              returnId={data.id}
              initialItemId={itemId}
              modalMode={mode}
              loadNextItem={nextItemId ? () => onOpen(nextItemId) : undefined}
            />
          )}

          <Toolbar />
          <DetailTabs tabs={tabs} />
          <SidePanel />
          <Footer />
        </TableProvider>
      ) : (
        <AlertModal
          open={true}
          onOk={() =>
            navigate(
              RouteBuilder.create(AppRoute.Distribution)
                .addPart(AppRoute.InboundReturn)
                .build()
            )
          }
          title={t('error.return-not-found')}
          message={t('messages.click-to-return-to-inbound-returns')}
        />
      )}
    </React.Suspense>
  );
};
