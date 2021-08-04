import React, { FC } from 'react';
import { LoadingApp } from '@openmsupply-client/common';
import TransactionService from './Service';

const App: FC = () => (
  <React.Suspense fallback={<LoadingApp />}>
    <TransactionService />
  </React.Suspense>
);

export default App;
