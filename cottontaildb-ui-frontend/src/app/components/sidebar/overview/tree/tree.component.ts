import {FlatTreeControl} from '@angular/cdk/tree';
import {Component, Input, OnInit} from '@angular/core';
import {MatTreeFlatDataSource, MatTreeFlattener} from '@angular/material/tree';
import {TreeNode} from "../../../../interfaces/TreeNode";
import {TreeDataService} from "../../../../services/tree-data.service";
import {Schema, SchemaService} from "../../../../services/schema.service";
import {EntityService} from "../../../../services/entity.service";
import {MatSnackBar} from '@angular/material/snack-bar';
import {MatDialog} from "@angular/material/dialog";
import {CreateEntityFormComponent} from "../create-entity-form/create-entity-form.component";
import {SelectionService} from "../../../../services/selection.service";

/** Flat node with expandable and level information */
interface FlatNode {
  expandable: boolean;
  name: string;
  level: number;
}

/**
 * @title Tree with flat nodes
 */
@Component({
  selector: 'app-tree',
  templateUrl: './tree.component.html',
  styleUrls: ['./tree.component.css'],
})

export class TreeComponent implements OnInit {


  private _transformer = (node: TreeNode, level: number) => {
    return {
      expandable: !!node.children && node.children.length > 0,
      name: node.name,
      level: level,
    };
  };

  treeControl = new FlatTreeControl<FlatNode>(
    node => node.level,
    node => node.expandable,
  );

  treeFlattener = new MatTreeFlattener(
    this._transformer,
    node => node.level,
    node => node.expandable,
    node => node.children,
  );

  @Input() connection : any

  dataSource = new MatTreeFlatDataSource(this.treeControl, this.treeFlattener)


  constructor(private treeDataService: TreeDataService,
              private schemaService : SchemaService,
              private entityService : EntityService,
              private _snackBar: MatSnackBar,
              private createEntityFormComponent : CreateEntityFormComponent,
              private dialog : MatDialog,
              private selectionService : SelectionService) {
  }

  ngOnInit() {
    this.treeDataService.fetchTreeData(this.connection);
    this.treeDataService.getTreeData().subscribe((datasource) => {
      return this.dataSource.data = datasource.get(this.connection) || [];
    });
  }

  isRoot = (_: number, node: FlatNode) => (node.level === 0);

  public onDropSchema(schema : Schema){
    if(confirm("are you sure you want to drop the schema " + schema + "?")){
      this.schemaService.dropSchema(this.connection, schema).subscribe({
        next: () => {
          this.treeDataService.fetchTreeData(this.connection);
          this._snackBar.open( "dropped schema successfully", "ok", {duration:2000})
        },
        error: () => this._snackBar.open("error", "ok", {duration:2000})
      });
    }
  }

  public onDropEntity(entityName : string){
    if(confirm("are you sure you want to drop the entity " + entityName + "?")){
      this.entityService.dropEntity(this.connection, entityName).subscribe({
        next: () => {
          this.treeDataService.fetchTreeData(this.connection);
          this._snackBar.open( "dropped entity successfully", "ok", {duration:2000})
        },
        error: () => this._snackBar.open("error", "ok", {duration:2000})
      });
    }
  }

  onTruncateEntity(entityName : string) {
    if(confirm("are you sure you want to delete the entity " + entityName + "?")){
      this.entityService.truncateEntity(this.connection, entityName).subscribe({
        next: () => {
          this.treeDataService.fetchTreeData(this.connection);
          this._snackBar.open( "truncated entity successfully", "ok", {duration:2000})
        },
        error: () => this._snackBar.open("error", "ok", {duration:2000})
      });
    }
  }

  onClearEntity(entityName : string) {
    if(confirm("are you sure you want to delete the entity " + entityName + "?")){
      this.entityService.clearEntity(this.connection, entityName).subscribe({
        next: () => {
          this.treeDataService.fetchTreeData(this.connection);
          this._snackBar.open( "cleared entity successfully", "ok", {duration:2000})
        },
        error: () => this._snackBar.open("error", "ok", {duration:2000})
      });
    }
  }

  onCreateEntity(name : string) {
    this.dialog.open<CreateEntityFormComponent>(CreateEntityFormComponent, {
      width: 'fit-content',
      height: 'fit-content',
      data: {
        name
      }
    });
  }

  onSelect(nodeName : string) {
    this.selectionService.changeSelection(this.connection, nodeName)
  }

  onDumpEntity(name: string) {
    //TODO
  }
}

