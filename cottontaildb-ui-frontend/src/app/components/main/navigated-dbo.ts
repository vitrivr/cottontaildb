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
     public readonly connection: String,
     public readonly schema: String | null = null,
     public readonly entity: String | null = null) {

    if (schema) {
      this.type = DboType.CONNECTION
    } else if (entity) {
      this.type = DboType.SCHEMA
    } else {
      this.type = DboType.ENTITY
    }
  }
}
