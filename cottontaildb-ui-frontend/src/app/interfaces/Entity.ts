
export interface Entity {

  schema : string;
  name : string;
  rows : number;

  //entity might not have any columns or indexes.
  columns?: string[];
  indexes?: string[];

}
