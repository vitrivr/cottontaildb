import { Component, OnInit } from '@angular/core';
import {MatTreeNode} from "@angular/material/tree";
import {CdkDragDrop} from "@angular/cdk/drag-drop";

@Component({
  selector: 'app-ddl-view',
  templateUrl: './ddl-view.component.html',
  styleUrls: ['./ddl-view.component.css']
})
export class DdlViewComponent implements OnInit {
  dropZoneText = "Drag and Drop Schema or Entity here";

  constructor() { }

  ngOnInit(): void {
  }

  drop($event: CdkDragDrop<MatTreeNode<any>, any>) {
    if($event.item.data.level === 0) {
      this.dropZoneText = $event.item.data.name;
    } else {
      this.dropZoneText = "1"
    }
  }
}
