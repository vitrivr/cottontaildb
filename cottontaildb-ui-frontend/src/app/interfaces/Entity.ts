import {Schema} from "./Schema";

export interface Entity {

  schema : Schema;
  name : string;
  rows : number;

  //entity might not have any columns or indexes.
  columns?: string[];
  indexes?: string[];

}
