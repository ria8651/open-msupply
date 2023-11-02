import { TemperatureBreachNodeType } from '@common/types';
import { TemperatureBreachRowFragment } from '../../api';
import { SensorFragment } from '../../../Sensor/api';

export interface PopoverVirtualElement {
  getBoundingClientRect: () => DOMRect;
  nodeType: Node['ELEMENT_NODE'];
}

export interface Breach {
  anchor: PopoverVirtualElement | null;
  date: Date;
  sensorId: string;
  type: TemperatureBreachNodeType;
  breachId: string;
  endDateTime: Date | null;
  startDateTime: Date;
}

export interface DotProps {
  cx: number;
  cy: number;
  payload: Log;
}

export type Log = {
  date: number;
  sensorId: string;
  temperature: number | null;
  breach: {
    row: TemperatureBreachRowFragment;
    sensor: Pick<SensorFragment, 'id' | 'name'>;
  } | null;
};

export type Sensor = {
  colour: string | undefined;
  id: string;
  name: string;
  location?: string | null;
  logs: Log[];
};

export type BreachDot = {
  position: DOMRect;
  breach: NonNullable<Log['breach']>;
};
