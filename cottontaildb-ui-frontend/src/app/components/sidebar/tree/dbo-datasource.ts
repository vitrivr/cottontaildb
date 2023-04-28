import {CollectionViewer, DataSource, SelectionChange} from "@angular/cdk/collections";
import {catchError, map, merge, Observable, Subject, Subscription} from "rxjs";
import {DboNode} from "./dbo-node";
import {FlatTreeControl, NestedTreeControl} from "@angular/cdk/tree";
import {ConnectionService} from "../../../services/connection.service";
import {SchemaService} from "../../../../../openapi";
import {DboNodeType} from "./dbo-node-type";

/**
 * File database, it can build a tree structured Json object from string.
 * Each node in Json object represents a file or a directory. For a file, it has filename and type.
 * For a directory, it has filename and children (a list of files or directories).
 * The input will be a json object string, and the output is a list of `FileNode` with nested
 * structure.
 */
export class DboDatasource implements DataSource<DboNode> {

  /** Internal array that represents the current state of this {@link DboDatasource}. */
  private _data = Array<DboNode>()

  /** The {@link Subject} holding the current state of the view.*/
  private dataChange = new Subject<null>();

  /** A {@link Subscription} for detection to changes ot the connection pool. */
  private _connectionSubscription: Subscription | null = null

  /**
   *
   * @param _treeControl
   * @param connections
   * @param schemas
   */
  constructor(
    private _treeControl: FlatTreeControl<DboNode>,
    private connections: ConnectionService,
    private schemas: SchemaService
  ) {
  }

  /**
   *
   * @param collectionViewer
   */
  public connect(collectionViewer: CollectionViewer): Observable<DboNode[]> {
    /* Subscription that synchronises connection state with view. */
    this._connectionSubscription = this.connections.connectionSubject.subscribe(connections => {
      for (let c of connections) {
        if (this._data.findIndex(v => v.type === DboNodeType.CONNECTION && v.name === `${c.host}:${c.port}`) == -1) {
          this._data.push(new DboNode(`${c.host}:${c.port}`, DboNodeType.CONNECTION, []))
        }
      }

      for (let c of this._data) {
        let index = connections.findIndex(v => c.name === `${v.host}:${v.port}`)
        if (index === -1) this._data.splice(index, 1)
      }
      this.dataChange.next(null)
    })

    /* Connect the tree control. */
    this._treeControl.expansionModel.changed.subscribe(change => {
      if ((change as SelectionChange<DboNode>).added) {
        change.added.forEach(node => this.refresh(node));
      }
      if ((change as SelectionChange<DboNode>).removed) {
        change.removed.forEach(node => node.children = []);
        this.dataChange.next(null)
      }
    });

    /* Return an observable that emits whenever the data has been changed. */
    return merge(collectionViewer.viewChange, this.dataChange).pipe(map(() => {
      const flattened = []
      for (let c of this._data) {
        flattened.push(c)
        for (let s of c.children) {
          flattened.push(s)
          for (let e of s.children) {
            flattened.push(e)
          }
        }
      }
      return flattened
    }));
  }


  /**
   *
   * @param collectionViewer
   */
  public disconnect(collectionViewer: CollectionViewer): void {
    this._connectionSubscription?.unsubscribe()
    this._connectionSubscription = null
  }

  /**
   * Toggle the node, remove from display list
   */
  public refresh(node: DboNode) {
    switch (node.type) {
      case DboNodeType.CONNECTION:
        this.refreshConnection(node);
        break;
      default:
        break;
    }
  }

  /**
   * Handles the expansion of the children of the given node.
   *
   * @param node
   * @private
   */
  public refreshConnection(node: DboNode) {
    if (node.type !== DboNodeType.CONNECTION) throw new Error("Cannot refresh connection for non-connection node.");
    node.isLoading = true
    this.schemas.getApiByConnectionList(node.name).pipe(
      catchError((err) => {
        console.error(err)
        node.children = []
        this.dataChange.next(null)
        return []
      }),
      map((schemas) => schemas.map((schema) => new DboNode(schema.name, DboNodeType.SCHEMA))),
    ).subscribe((schemas) => {
        node.mergeChildren(schemas)
        this.dataChange.next(null)
        node.isLoading = false
    })
  }
}
