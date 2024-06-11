import React from 'react';
import {
  Collapse,
  List,
  useTranslation,
  RouteBuilder,
  AppNavLink,
  AppNavSection,
  useIsCentralServerApi,
  UserStoreNodeFragment,
} from '@openmsupply-client/common';
import { AppRoute } from '@openmsupply-client/config';
import { useNestedNav } from './useNestedNav';
import { PowerIcon } from '@openmsupply-client/common/src/ui/icons/Power';

export const ProgramsNav = ({ store }: { store?: UserStoreNodeFragment }) => {
  const { isActive } = useNestedNav(
    RouteBuilder.create(AppRoute.Programs).addWildCard().build()
  );
  const t = useTranslation('app');
  const isCentralServer = useIsCentralServerApi();
  const visible = isCentralServer && store?.preferences.vaccineModule;

  return (
    <AppNavSection isActive={isActive} to={AppRoute.Programs}>
      <AppNavLink
        visible={visible}
        end={false}
        to={AppRoute.Programs}
        icon={<PowerIcon color="primary" fontSize="small" />}
        text={t('programs')}
        inactive
      />
      <Collapse in={isActive}>
        <List>
          <AppNavLink
            visible={visible}
            end
            to={RouteBuilder.create(AppRoute.Programs)
              .addPart(AppRoute.ImmunisationPrograms)
              .build()}
            text={t('label.programs-immunisations')}
          />
        </List>
      </Collapse>
    </AppNavSection>
  );
};
