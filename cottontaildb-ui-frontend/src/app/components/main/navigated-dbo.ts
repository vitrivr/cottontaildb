import {DboType} from "../../model/dbo/dbo-type";

/**
 * A representation of the current navigation state with respect to selected Dbo.
 */
export class NavigatedDbo {

  /** The {@link DboType} for this {@link NavigatedDbo}. */
  public readonly type: DboType

  /**
   *
   * @param connection
   * @param schema
   * @param entity
   */
  constructor(
     public readonly connection: string,
     public readonly schema: String | null = null,
     public readonly entity: String | null = null) {

    if (this.schema == null && this.entity == null) {
      this.type = DboType.CONNECTION
    } else if (this.entity == null) {
      this.type = DboType.SCHEMA
    } else {
      this.type = DboType.ENTITY
    }
    console.log(this)
  }
}
