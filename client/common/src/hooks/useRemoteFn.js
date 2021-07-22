import { useState, useEffect } from 'react';
import { useRemoteScript } from './useRemoteScript';

export const loadAndInjectDeps = (scope, module) => {
  return async () => {
    // Initializes the share scope. This fills it with known provided modules from this build and all remotes
    await __webpack_init_sharing__('default');
    const container = window[scope]; // or get the container somewhere else
    // Initialize the container, it may provide shared modules
    await container.init(__webpack_share_scopes__.default);
    const factory = await window[scope].get(module);
    const Module = factory();
    return Module;
  };
};

const placeholderFn = () => {
  alert('fn is still loading!');
};

export const useRemoteFn = (url, scope, module) => {
  const { ready, failed } = useRemoteScript(url);
  const [fn, setFn] = useState(() => placeholderFn);

  useEffect(() => {
    const getFn = async () => {
      const loaded = loadAndInjectDeps(scope, module);
      const remoteFn = await loaded();

      // Note: Using a straight setFn(loaded.default) will
      // result in undefined as `setState` will treat the fn as
      // a functional update. i.e. setState(state => newState)
      setFn(() => remoteFn.default);
    };

    if (ready) {
      getFn();
    }
  }, [ready]);

  if (!ready) {
    return { ready, failed };
  }

  if (failed) {
    return { ready, failed };
  }

  return { ready, failed, fn };
};