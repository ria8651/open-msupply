import { StocktakeLineNode } from './../../common/src/types/schema';
import { StocktakeNode, StocktakeNodeStatus } from '@openmsupply-client/common';

export type StocktakeRow = Pick<
  StocktakeNode,
  | 'id'
  | 'comment'
  | 'description'
  | 'stocktakeDatetime'
  | 'stocktakeNumber'
  | 'status'
>;

export interface StocktakeLine extends StocktakeLineNode {
  isCreated?: boolean;
  isDeleted?: boolean;
  isUpdated?: boolean;
}

export interface StocktakeItem {
  id: string;
  itemCode: string;
  itemName: string;
  isDeleted?: boolean;
  lines: StocktakeLine[];
}

export interface Stocktake
  extends Omit<
    StocktakeNode,
    'lines' | '__typename' | 'stocktakeDatetime' | 'entryDatetime'
  > {
  lines: StocktakeLine[];
  stocktakeDatetime: Date;
  entryDatetime: Date;
}

export interface StocktakeController extends Omit<Stocktake, 'lines'> {
  lines: StocktakeItem[];
  update: (key: string, value: string) => void;
  updateStocktakeDatetime: (newDate: Date | null) => void;
  updateOnHold: () => void;
  updateStatus: (newStatus: StocktakeNodeStatus) => void;
}

export enum StocktakeActionType {
  Update = 'Stocktake/Update',
  UpdateStocktakeDatetime = 'Stocktake/UpdateStocktakeDatetime',
  UpdateOnHold = 'Stocktake/UpdateOnHold',
  UpdateStatus = 'Stocktake/UpdateStatus',
}

export type StocktakeAction =
  | {
      type: StocktakeActionType.Update;
      payload: { key: string; value: string };
    }
  | {
      type: StocktakeActionType.UpdateStocktakeDatetime;
      payload: { newDate: Date | null };
    }
  | {
      type: StocktakeActionType.UpdateOnHold;
    }
  | {
      type: StocktakeActionType.UpdateStatus;
      payload: { newStatus: StocktakeNodeStatus };
    };