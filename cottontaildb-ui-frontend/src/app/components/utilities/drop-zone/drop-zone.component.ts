import {Component, EventEmitter, OnInit, Output} from '@angular/core';
import {CdkDragDrop} from "@angular/cdk/drag-drop";
import {MatTreeNode} from "@angular/material/tree";

@Component({
  selector: 'app-drop-zone',
  templateUrl: './drop-zone.component.html',
  styleUrls: ['./drop-zone.component.css']
})
export class DropZoneComponent implements OnInit {

  constructor() {
  }

  ngOnInit(): void {
  }

  @Output() itemDroppedEvent = new EventEmitter<string>();


  drop($event: CdkDragDrop<MatTreeNode<any>, any>) {
    this.itemDroppedEvent.emit($event.item.data.name)
  }

}

