import {DboNodeType} from "./dbo-node-type";
import {Connection, Schema} from "../../../../../openapi";

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
    public readonly type: DboNodeType,
    public readonly children: DboNode[] = [],
    public parent: DboNode | undefined = undefined,
    public isLoading: boolean = false,
    public context: Connection | Schema | undefined = undefined
  ) {

  }

  /**
   *
   * @param child
   */
  public addChild(child : DboNode){
    switch (this.type) {
      case DboNodeType.CONNECTION:
        if (child.type == DboNodeType.SCHEMA) {
          this.children.push(child)
          child.parent = this
        }
        break;
      case DboNodeType.SCHEMA:
        if (child.type == DboNodeType.ENTITY) {
          this.children.push(child)
          child.parent = this
        }
        break;
      case DboNodeType.ENTITY:
        if (child.type == DboNodeType.INDEX || child.type == DboNodeType.COLUMN) {
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
    return (this.type == DboNodeType.CONNECTION || this.type == DboNodeType.SCHEMA || this.type == DboNodeType.ENTITY)
  }

  /**
   *
   */
  get hasChild(): boolean {
    switch (this.type) {
      case DboNodeType.CONNECTION:
        return true;
      case DboNodeType.ENTITY:
        return true;
      case DboNodeType.SCHEMA:
        return true;
      case DboNodeType.INDEX:
        return false;
      case DboNodeType.COLUMN:
        return false;
      default:
        return false
    }
  }
}
