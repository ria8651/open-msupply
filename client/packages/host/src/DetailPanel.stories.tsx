import React, { useEffect } from 'react';
import { ComponentStory, ComponentMeta } from '@storybook/react';
import DetailPanel from './DetailPanel';
import {
  Action,
  Section,
  useDetailPanel,
} from '@openmsupply-client/common/src/hooks/useDetailPanel';
import { Typography } from '@openmsupply-client/common/src/ui/components/index';
import { TestingProvider } from '@openmsupply-client/common/src/utils/testing';
import Grid from '@material-ui/core/Grid';

export default {
  title: 'Host/DetailPanel',
  component: DetailPanel,
} as ComponentMeta<typeof DetailPanel>;

const Template: ComponentStory<typeof DetailPanel> = () => {
  const { OpenButton, setActions, setSections } = useDetailPanel();
  const actions: Action[] = [
    { titleKey: 'link.backorders', onClick: () => {} },
  ];
  const sections: Section[] = [
    {
      titleKey: 'heading.comment',
      children: [
        <Typography key="0">comments to be shown in here...</Typography>,
      ],
    },
    {
      titleKey: 'heading.additional-info',
      children: [<Typography key="0">additional info...</Typography>],
    },
  ];

  useEffect(() => setActions(actions), []);
  useEffect(() => setSections(sections), []);

  return (
    <TestingProvider locale="en">
      <Grid container>
        <Grid item flex={1}>
          {OpenButton}
        </Grid>
        <Grid item>
          <DetailPanel />
        </Grid>
      </Grid>
    </TestingProvider>
  );
};

export const Primary = Template.bind({});
