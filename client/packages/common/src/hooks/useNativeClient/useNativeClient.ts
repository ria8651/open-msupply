import { uniqWith } from 'lodash';
import { useState, useEffect } from 'react';
import { KeepAwake } from '@capacitor-community/keep-awake';
import {
  getNativeAPI,
  getPreference,
  matchUniqueServer,
  setPreference,
} from './helpers';
import {
  ConnectionResult,
  DISCOVERED_SERVER_POLL,
  DISCOVERY_TIMEOUT,
  FrontEndHost,
  NativeAPI,
  NativeMode,
} from './types';
import { Capacitor } from '@capacitor/core';

declare global {
  interface Window {
    electronNativeAPI: NativeAPI;
  }
}

type NativeClientState = {
  // A previous server is set in local storage, but was not returned in the list of available servers
  connectToPreviousFailed: boolean;
  connectedServer: FrontEndHost | null;
  // Indicate that server discovery has taken too long without finding server
  discoveryTimedOut: boolean;
  isDiscovering: boolean;
  previousServer: FrontEndHost | null;
  servers: FrontEndHost[];
};

export const useNativeClient = ({
  autoconnect,
  discovery,
}: { discovery?: boolean; autoconnect?: boolean } = {}) => {
  const nativeAPI = getNativeAPI();

  const setMode = (mode: NativeMode) =>
    setPreference('mode', mode).then(() =>
      setState(state => ({ ...state, mode }))
    );

  const [state, setState] = useState<NativeClientState>({
    connectToPreviousFailed: false,
    connectedServer: null,
    discoveryTimedOut: false,
    isDiscovering: false,
    previousServer: null,
    servers: [],
  });

  const connectToServer = (server: FrontEndHost): Promise<ConnectionResult> => {
    setPreference('previousServer', server);
    return (
      nativeAPI?.connectToServer(server) ?? Promise.resolve({ success: false })
    );
  };

  const stopDiscovery = () =>
    setState(state => ({ ...state, isDiscovering: false }));

  const startDiscovery = () => {
    if (!nativeAPI) return;

    nativeAPI.connectedServer().then(connectedServer => {
      setState(state => ({ ...state, connectedServer }));
    });

    if (!discovery) return;

    setState(state => ({
      ...state,
      servers: [],
      discoveryTimedOut: false,
      isDiscovering: true,
    }));

    nativeAPI.startServerDiscovery();
  };

  const readLog = async () => {
    const noResult = 'log unavailable';
    const result = await nativeAPI?.readLog();

    if (!result) return noResult;
    if (result.error) console.error(result.error);

    return result.log || noResult;
  };

  const allowSleep = async () => {
    // Currently only supported on native platforms via capacitor
    if (!Capacitor.isNativePlatform) return;

    const result = await KeepAwake.isSupported();
    if (result.isSupported) await KeepAwake.allowSleep();
  };

  const keepAwake = async () => {
    // Currently only supported on native platforms via capacitor
    if (!Capacitor.isNativePlatform) return;

    const result = await KeepAwake.isSupported();
    if (result.isSupported) await KeepAwake.keepAwake();
  };

  const handleConnectionResult = (result: ConnectionResult) => {
    if (result.success) return;

    setState(state => ({ ...state, connectToPreviousFailed: true }));
    console.error('Connecting to previous server:', result.error);
  };

  useEffect(() => {
    if (!state.isDiscovering) return;

    const timeoutTimer = setTimeout(() => {
      setState(state => ({
        ...state,
        discoveryTimedOut: true,
      }));
      clearInterval(pollInterval);
    }, DISCOVERY_TIMEOUT);

    const pollInterval = setInterval(async () => {
      const servers = (await nativeAPI?.discoveredServers())?.servers || [];
      if (servers.length === 0) return;

      setState(state => ({
        ...state,
        servers: uniqWith([...state.servers, ...servers], matchUniqueServer),
        discoveryTimedOut: false,
      }));

      clearTimeout(timeoutTimer);
    }, DISCOVERED_SERVER_POLL);

    return () => {
      clearTimeout(timeoutTimer);
      clearInterval(pollInterval);
    };
  }, [state.isDiscovering]);

  useEffect(() => {
    startDiscovery();
    getPreference('previousServer', '').then(server => {
      if (!!server) setState(state => ({ ...state, previousServer: server }));
    });
  }, []);

  // Auto connect if autoconnect=true and server found matching previousConnectedServer
  useEffect(() => {
    const { previousServer } = state;
    if (!nativeAPI) return;
    if (!autoconnect) return;
    if (previousServer === null) return;

    // this will check to see if the server is alive and if so, connect to it
    connectToServer(previousServer)
      .then(handleConnectionResult)
      .catch(e => handleConnectionResult({ success: false, error: e.message }));
  }, [state.previousServer, autoconnect]);

  return {
    ...state,
    connectToServer,
    goBackToDiscovery: nativeAPI?.goBackToDiscovery ?? (() => {}),
    advertiseService: nativeAPI?.advertiseService ?? (() => {}),
    startDiscovery,
    stopDiscovery,
    setMode,
    readLog,
    keepAwake,
    allowSleep,
  };
};
