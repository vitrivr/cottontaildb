import {Component, OnDestroy, OnInit} from '@angular/core';
import {ConnectionService} from "../../services/connection.service";
import {CreateSchemaFormComponent} from "./create-schema-form/create-schema-form.component";
import {AddConnectionFormComponent} from "./add-connection-form/add-connection-form.component";
import {MatDialog} from "@angular/material/dialog";
import {MatTreeNestedDataSource} from "@angular/material/tree";
import {DboNode} from "./tree/dbo-node";
import {NestedTreeControl} from "@angular/cdk/tree";
import {DboNodeType} from "./tree/dbo-node-type";
import {Connection} from "../../../../openapi";
import {Subscription} from "rxjs";

@Component({
  selector: 'app-sidebar',
  templateUrl: './sidebar.component.html',
  styleUrls: ['./sidebar.component.css']
})
export class SidebarComponent implements OnInit, OnDestroy {


  /** */
  private _connectionSubscription: Subscription

  /** The raw {@link DboNode} array that backs the sidebar tree. */
  private readonly _data: Array<DboNode> = []

  /** The {@link MatTreeNestedDataSource} used as data source for the sidebar tree. */
  public readonly dataSource = new MatTreeNestedDataSource<DboNode>();

  /** The {@link NestedTreeControl} used as control for the sidebar tree. */
  public readonly treeControl = new NestedTreeControl<DboNode>(node=> node.children);

  /**
   *
   * @param dialog
   * @param connections
   */
  constructor(private dialog: MatDialog, private connections: ConnectionService) {
    /* Subscription that synchronises connection state with view. */
    this._connectionSubscription = this.connections.connectionSubject.subscribe(connections => {
      for (let c of connections) {
        if (this._data.findIndex(v => v.type === DboNodeType.CONNECTION && v.name === `${c.host}:${c.port}`) == -1) {
          this._data.push(new DboNode(`${c.host}:${c.port}`, DboNodeType.CONNECTION, []))
        }
      }

      for (let c of this._data) {
        let index = connections.findIndex(v => c.name === `${v.host}:${v.port}`)
        if (index === -1) {
          this._data.splice(index, 1)
        }
      }

      this.dataSource.data = this._data
    })
  }

  /**
   *
   */
  ngOnInit(): void {
    this.connections.refresh()
  }

  /**
   *
   */
  ngOnDestroy() {
    this._connectionSubscription.unsubscribe()
  }

  /**
   *
   * @param node
   */
  public hasChild(index: number, node: DboNode) {
    return true;
  }

  /**
   * Loads the children of the given node.
   *
   * @param node
   * @private
   */
  private refreshChildren(node: DboNode): DboNode[] {
    return []
  }

  /**
   *
   */
  public createConnection() {
    let ref = this.dialog.open<AddConnectionFormComponent>(AddConnectionFormComponent, {
      width: 'fit-content',
      height: 'fit-content',
    });
    ref.afterClosed().subscribe((result: Connection) => this.connections.connect(result))
  }


  /**
   *
   * @param connection
   */
  onCreateSchema(connection: Connection) {
    let ref = this.dialog.open<CreateSchemaFormComponent>(CreateSchemaFormComponent, {width: 'fit-content', height: 'fit-content'})
    ref.afterClosed().subscribe((result: Connection) => this.connections.connect(result))
  }
  onRemoveConnection(connection: any) {
    this.connections.disconnect(connection)
  }
}
