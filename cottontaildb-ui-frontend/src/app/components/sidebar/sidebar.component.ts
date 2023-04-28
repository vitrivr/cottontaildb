import {ChangeDetectorRef, Component, OnDestroy, OnInit} from '@angular/core';
import {ConnectionService} from "../../services/connection.service";
import {CreateSchemaFormComponent} from "./create-schema-form/create-schema-form.component";
import {AddConnectionFormComponent} from "./add-connection-form/add-connection-form.component";
import {MatDialog} from "@angular/material/dialog";
import {MatTreeNestedDataSource} from "@angular/material/tree";
import {DboNode} from "./tree/dbo-node";
import {FlatTreeControl, NestedTreeControl} from "@angular/cdk/tree";
import {DboNodeType} from "./tree/dbo-node-type";
import {Connection, SchemaService} from "../../../../openapi";
import {catchError} from "rxjs";
import {MatSnackBar, MatSnackBarConfig} from "@angular/material/snack-bar";
import {DboDatasource} from "./tree/dbo-datasource";

@Component({
  selector: 'app-sidebar',
  templateUrl: './sidebar.component.html',
  styleUrls: ['./sidebar.component.css']
})
export class SidebarComponent implements OnInit {

  /** The {@link NestedTreeControl} used as control for the sidebar tree. */
  public readonly treeControl = new FlatTreeControl<DboNode,DboNode>(
    (node: DboNode) => node.level,
    (node) => node.expandable
  );

  /** The {@link MatTreeNestedDataSource} used as data source for the sidebar tree. */
  public readonly dataSource = new DboDatasource(this.treeControl, this.connections, this.schemas);

  /**
   *
   * @param dialog
   * @param _snackBar
   * @param cdr
   * @param connections
   * @param schemas
   */
  constructor(private dialog: MatDialog, private _snackBar: MatSnackBar, private cdr: ChangeDetectorRef, private connections: ConnectionService, private schemas: SchemaService) {

  }

  /**
   *
   */
  ngOnInit(): void {
    this.connections.refresh()
  }

  /**
   * Returns true if provided {@link DboNode} represents a connection.
   *
   * @param index The index.
   * @param node The {@link DboNode} to check.
   */
  public isConnection(index: number, node: DboNode): boolean {
    return node.type === DboNodeType.CONNECTION
  }

  /**
   * Returns true if provided {@link DboNode} represents a schema.
   *
   * @param index The index.
   * @param node The {@link DboNode} to check.
   */
  public isSchema(index: number, node: DboNode): boolean {
    return node.type === DboNodeType.SCHEMA
  }

  /**
   * Opens the form to create a new connection and establishes the connection upon completion.
   */
  public connect() {
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
  public disconnect(connection: Connection) {
    this.connections.disconnect(connection)
  }

  /**
   * Opens the form to create a new schema and creates it upon completion.
   */
  public createSchema(node: DboNode) {
    if (node.type !== DboNodeType.CONNECTION) throw new Error("Cannot create schema for non-connection node.");
    let ref = this.dialog.open<CreateSchemaFormComponent>(CreateSchemaFormComponent, {width: 'fit-content', height: 'fit-content'})
    ref.afterClosed().subscribe((result: string) => {
      if (result) {
        this.schemas.postApiByConnectionBySchema(node.name, result).pipe(
          catchError((err) => {
            console.log(err);
            let snackBarRef = this._snackBar.open(`Error occurred when trying to create schema ${result}: ${err.message}.`, "Dismiss", { duration: 2000 } as MatSnackBarConfig);
            return []
          })).subscribe(r => {
            let snackBarRef = this._snackBar.open(`Schema ${result} created successfully.`, "Dismiss", { duration: 2000 } as MatSnackBarConfig);
            this.dataSource.refreshConnection(node); /* Reload children. */
        })
      }
    })
  }
}
