import {Connection, Dbo} from "../../../../../openapi";
import {DboType} from "../../../model/dbo/dbo-type";

/**
 * An enumeration of the different DBO node types displayed in the tree view.
 *
 * @author Ralph Gasser
 */
export class DboNode {

  /**
   *
   * @param name
   * @param type
   * @param children
   * @param parent
   * @param isLoading
   * @param context
   */
  constructor(
    public readonly name: string,
    public readonly type: DboType,
    public readonly children: DboNode[] = [],
    public parent: DboNode | undefined = undefined,
    public isLoading: boolean = false,
    public context: Connection | Dbo | undefined = undefined
  ) {

  }

  /**
   *
   * @param child
   */
  public addChild(child : DboNode){
    switch (this.type) {
      case DboType.CONNECTION:
        if (child.type == DboType.SCHEMA) {
          this.children.push(child)
          child.parent = this
        }
        break;
      case DboType.SCHEMA:
        if (child.type == DboType.ENTITY) {
          this.children.push(child)
          child.parent = this
        }
        break;
      case DboType.ENTITY:
        if (child.type == DboType.INDEX || child.type == DboType.COLUMN) {
          this.children.push(child)
          child.parent = this
        }
        break;
      default:
    }
  }

  /**
   * Merges the children of this {@link DboNode} with the provided {@link DboNode}s.
   *
   * @param children The {@link DboNode}s to merge.
   */
  public mergeChildren(children: DboNode[]) {
    for (const child of children) {
      if (this.children.findIndex(v => v.name === child.name) == -1) {
        this.addChild(child)
      }
    }

    for (const [index, child] of this.children.entries()) {
      if (children.findIndex(v => v.name === child.name) == -1) {
        this.children.splice(index, 1)
      }
    }
  }

  /**
   *
   */
  get level(): number {
    return this.type.valueOf()
  }

  /**
   *
   */
  get expandable(): boolean {
    return (this.type == DboType.CONNECTION || this.type == DboType.SCHEMA || this.type == DboType.ENTITY)
  }
}
