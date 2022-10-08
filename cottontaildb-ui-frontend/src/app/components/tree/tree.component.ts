import {FlatTreeControl} from '@angular/cdk/tree';
import {Component, OnInit} from '@angular/core';
import {MatTreeFlatDataSource, MatTreeFlattener} from '@angular/material/tree';
import {CdkDragEnd} from "@angular/cdk/drag-drop";
import {TreeNode} from "../../interfaces/TreeNode";
import {TreeDataService} from "../../services/tree-data.service";

/**
 * Food data with nested structure.
 * Each node has a name and an optional list of children.
 */


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

  constructor(private treeDataService: TreeDataService) {

  }

  ngOnInit() {
    this.treeDataService.getTreeData().subscribe((datasource) => this.dataSource.data = datasource)
  }


  hasChild = (_: number, node: ExampleFlatNode) => node.expandable;


}

