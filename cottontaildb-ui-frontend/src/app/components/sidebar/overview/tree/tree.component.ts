import {FlatTreeControl} from '@angular/cdk/tree';
import {Component, OnInit} from '@angular/core';
import {MatTreeFlatDataSource, MatTreeFlattener} from '@angular/material/tree';
import {TreeNode} from "../../../../interfaces/TreeNode";
import {TreeDataService} from "../../../../services/tree-data.service";
import {Schema, SchemaService} from "../../../../services/schema.service";
import {EntityService} from "../../../../services/entity.service";
import {MatSnackBar} from '@angular/material/snack-bar';



/** Flat node with expandable and level information */
interface ExampleFlatNode {
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

  treeControl = new FlatTreeControl<ExampleFlatNode>(
    node => node.level,
    node => node.expandable,
  );

  treeFlattener = new MatTreeFlattener(
    this._transformer,
    node => node.level,
    node => node.expandable,
    node => node.children,
  );


  dataSource = new MatTreeFlatDataSource(this.treeControl, this.treeFlattener);

  constructor(private treeDataService: TreeDataService,
              private schemaService : SchemaService,
              private entityService : EntityService,
              private _snackBar: MatSnackBar) {

  }



  ngOnInit() {
    this.treeDataService.fetchTreeData();
    this.treeDataService.getTreeData().subscribe((datasource) => this.dataSource.data = datasource);
  }

  isRoot = (_: number, node: ExampleFlatNode) => (node.level === 0);

  public onDropSchema(schema : Schema){
    if(confirm("are you sure you want to drop the schema " + schema + "?")){
      this.schemaService.dropSchema(schema).subscribe({
        next: () => {
          this.treeDataService.fetchTreeData();
          this._snackBar.open( "dropped schema successfully", "ok", {duration:2000})
        },
        error: () => this._snackBar.open("error", "ok", {duration:2000})
      });
    }
  }

  public onDropEntity(entityName : string){
    if(confirm("are you sure you want to drop the entity " + entityName + "?")){
      console.log(entityName)
      this.entityService.dropEntity(entityName).subscribe({
        next: () => {
          this.treeDataService.fetchTreeData();
          this._snackBar.open( "dropped entity successfully", "ok", {duration:2000})
        },
        error: () => this._snackBar.open("error", "ok", {duration:2000})
      });
    }
  }

  onTruncateEntity(entityName : string) {
    if(confirm("are you sure you want to delete the entity " + entityName + "?")){
      console.log(entityName)
      this.entityService.truncateEntity(entityName).subscribe({
        next: () => {
          this.treeDataService.fetchTreeData();
          this._snackBar.open( "truncated entity successfully", "ok", {duration:2000})
        },
        error: () => this._snackBar.open("error", "ok", {duration:2000})
      });
    }
  }

  onClearEntity(entityName : string) {
    if(confirm("are you sure you want to delete the entity " + entityName + "?")){
      console.log(entityName)
      this.entityService.clearEntity(entityName).subscribe({
        next: () => {
          this.treeDataService.fetchTreeData();
          this._snackBar.open( "cleared entity successfully", "ok", {duration:2000})
        },
        error: () => this._snackBar.open("error", "ok", {duration:2000})
      });
    }
  }
}

