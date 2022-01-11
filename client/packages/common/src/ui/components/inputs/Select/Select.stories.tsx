import React from 'react';
import { ComponentStory, ComponentMeta } from '@storybook/react';

import { Select } from './Select';
import { Grid, Typography } from '@mui/material';

export default {
  title: 'Inputs/Select',
  component: Select,
} as ComponentMeta<typeof Select>;

const Template: ComponentStory<typeof Select> = args => (
  <Grid container spacing={5} flexDirection="column">
    <Grid item>
      <Typography>Basic Select</Typography>
      <Select {...args} />
    </Grid>
    <Grid item>
      <Typography>Disabled</Typography>
      <Select {...args} disabled defaultValue={args.options[0]?.value ?? ''} />
    </Grid>
  </Grid>
);

const toOption = (value: string) => ({ label: value, value });

export const Primary = Template.bind({});
Primary.args = { options: ['eenie', 'meenie', 'miney', 'mo'].map(toOption) };
