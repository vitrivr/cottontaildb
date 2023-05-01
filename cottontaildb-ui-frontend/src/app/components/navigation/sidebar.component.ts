import {Component, OnInit} from '@angular/core';
import {ConnectionService} from "../../services/connection.service";
import {CreateSchemaFormComponent} from "./create-schema-form/create-schema-form.component";
import {AddConnectionFormComponent} from "./add-connection-form/add-connection-form.component";
import {MatTreeNestedDataSource} from "@angular/material/tree";
import {DboNode} from "./tree/dbo-node";
import {FlatTreeControl, NestedTreeControl} from "@angular/cdk/tree";
import {Connection, Dbo, EntityService, SchemaService} from "../../../../openapi";
import {catchError} from "rxjs";
import {DboDatasource} from "./tree/dbo-datasource";
import {ActivatedRoute, Params, Router} from "@angular/router";
import {DboType} from "../../model/dbo/dbo-type";
import {MatDialog} from "@angular/material/dialog";
import {MatSnackBar, MatSnackBarConfig} from "@angular/material/snack-bar";

@Component({
  selector: 'app-sidebar',
  templateUrl: './sidebar.component.html',
  styleUrls: ['./sidebar.component.scss']
})
export class SidebarComponent implements OnInit {

  /** The {@link NestedTreeControl} used as control for the sidebar tree. */
  public readonly treeControl = new FlatTreeControl<DboNode,DboNode>(
    (node: DboNode) => node.level,
    (node) => node.expandable
  );

  /** The currently selected {@link DboNode}. */
  public selectedNode: DboNode | undefined = undefined

  /** The {@link MatTreeNestedDataSource} used as data source for the sidebar tree. */
  public readonly dataSource = new DboDatasource(this.treeControl, this.connections, this.schemas, this.entities);
  Connection: void;

  /**
   *
   * @param dialog
   * @param _snackBar
   * @param router
   * @param activatedRoute
   * @param connections
   * @param schemas
   * @param entities
   */
  constructor(private dialog: MatDialog,
              private _snackBar: MatSnackBar,
              private router: Router,
              private activatedRoute: ActivatedRoute,
              private connections: ConnectionService,
              private schemas: SchemaService,
              private entities: EntityService
  ) {}

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
    return node.type === DboType.CONNECTION
  }

  /**
   * Returns true if provided {@link DboNode} represents a schema.
   *
   * @param index The index.
   * @param node The {@link DboNode} to check.
   */
  public isSchema(index: number, node: DboNode): boolean {
    return node.type === DboType.SCHEMA
  }

  /**
   * Returns true if provided {@link DboNode} represents a schema.
   *
   * @param index The index.
   * @param node The {@link DboNode} to check.
   */
  public isEntity(index: number, node: DboNode): boolean {
    return node.type === DboType.ENTITY
  }

  /**
   * Handles selection of a {@link DboNode} in the sidebar. Updates the navigation state accordingly.
   *
   * @param node The {@link DboNode} that was selected.
   */
  public nodeSelected(node: DboNode) {
    const queryParams: Params = { };
    switch (node.type) {
      case DboType.CONNECTION:
        queryParams['connection'] = node.name
        break;
      case DboType.SCHEMA:
        queryParams['connection'] = node.parent!!.name
        queryParams['schema'] = node.name
        break;
      case DboType.ENTITY:
        queryParams['connection'] = node.parent!!.parent!!.name
        queryParams['schema'] = node.parent!!.name
        queryParams['entity'] = node.name
        break;
    }
    this.selectedNode = node
    this.router.navigate([], {relativeTo: this.activatedRoute, queryParams: queryParams, queryParamsHandling: ''})
  }

  /**
   * Opens the form to create a new connection and establishes the connection upon completion.
   */
  public connect() {
    let ref = this.dialog.open<AddConnectionFormComponent>(AddConnectionFormComponent, {width: 'fit-content', height: 'fit-content',});
    ref.afterClosed().subscribe((result: Connection) => {
      if (result)this.connections.connect(result);
    })
  }

  /**
   * Opens a form to create a new schema using the {@link Connection} represented by the provided {@link DboNode}.
   *
   * @param node {@link DboNode}
   */
  public createSchema(node: DboNode) {
    if (node.type !== DboType.CONNECTION) throw new Error("Cannot create schema for non-connection node.");
    let ref = this.dialog.open<CreateSchemaFormComponent>(CreateSchemaFormComponent, {width: 'fit-content', height: 'fit-content'})
    ref.afterClosed().subscribe((result: string) => {
      if (result) {
        this.schemas.postApiByConnectionBySchema(node.name, result).pipe(
          catchError((err) => {
            this._snackBar.open(`Error occurred when trying to create schema '${result}': ${err.error.description}.`, "Dismiss", { duration: 2000 } as MatSnackBarConfig);
            return []
          })).subscribe(r => {
            this._snackBar.open(`Schema ${result} created successfully.`, "Dismiss", { duration: 2000 } as MatSnackBarConfig);
            this.dataSource.refreshConnection(node); /* Reload children. */
        })
      }
    })
  }

  /**
   * Drops the schema represented by the provided {@link DboNode}
   *
   * @param node {@link DboNode}
   */
  public dropSchema(node: DboNode) {
    if (node.type !== DboType.SCHEMA) throw new Error("Cannot drop schema for non-schema node.");
    this.schemas.deleteApiByConnectionBySchema(node.parent!!.name, (node.context!! as Dbo).name).pipe(
      catchError((err) => {
        this._snackBar.open(`Error occurred when trying to create schema '${node.name}': ${err.error.description}.`, "Dismiss", { duration: 2000 } as MatSnackBarConfig);
        return []
      })).subscribe(r => {
        this._snackBar.open(`Schema ${node.name} dropped successfully.`, "Dismiss", { duration: 2000 } as MatSnackBarConfig);
        this.dataSource.refreshConnection(node.parent!!); /* Reload children. */
    })
  }

  /**
   * Drops the schema represented by the provided {@link DboNode}
   *
   * @param node {@link DboNode}
   */
  public dropEntity(node: DboNode) {
    if (node.type !== DboType.ENTITY) throw new Error("Cannot execute dropEntity() for for non-entity node.");
    this.entities.deleteApiByConnectionBySchemaByEntity(node.parent!!.parent!!.name, node.parent!!.name, node.name).pipe(
      catchError((err) => {
        this._snackBar.open(`Error occurred when trying to drop entity '${node.name}': ${err.error.description}.`, "Dismiss", { duration: 2000 } as MatSnackBarConfig);
        return []
      })).subscribe(r => {
      this._snackBar.open(`Entity ${node.name} dropped successfully.`, "Dismiss", { duration: 2000 } as MatSnackBarConfig);
      this.dataSource.refreshSchema(node.parent!!); /* Reload children. */
    })
  }

  /**
   * Disconnects the {@link Connection} represented by the provided {@link DboNode}.
   *
   * @param node {@link DboNode}
   */
  public disconnect(node: DboNode) {
    if (node.type !== DboType.CONNECTION) throw new Error("Cannot execute disconnect() for a non-connection node.");
    this.connections.disconnect(node.context as Connection)
  }
}
