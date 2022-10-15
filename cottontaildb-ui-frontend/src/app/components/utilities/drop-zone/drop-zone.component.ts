import { Component, OnInit } from '@angular/core';
import {CdkDragDrop} from "@angular/cdk/drag-drop";
import {MatTreeNode} from "@angular/material/tree";
import {Router} from "@angular/router";

@Component({
  selector: 'app-drop-zone',
  templateUrl: './drop-zone.component.html',
  styleUrls: ['./drop-zone.component.css']
})
export class DropZoneComponent implements OnInit {

  constructor(private router : Router) {
  }

  ngOnInit(): void {
  }

  drop($event: CdkDragDrop<MatTreeNode<any>, any>) {
    if ($event.item.data.level === 0) {
      this.router.navigate(['schema']).then(r => console.log(r));
    } else {
      this.router.navigate(['entity']).then(r => console.log(r));    }
  }
}

