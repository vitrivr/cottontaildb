import { Component, OnInit } from '@angular/core';
import {SelectionService} from "../../../../services/selection.service";
import {EntityService} from "../../../../services/entity.service";


@Component({
  selector: 'app-dml-view',
  templateUrl: './dml-view.component.html',
  styleUrls: ['./dml-view.component.css']
})
export class DmlViewComponent implements OnInit {

  aboutEntityData: any;
  selection: any;

  constructor(private selectionService: SelectionService,
              private entityService: EntityService) { }

  ngOnInit(): void {
    this.selectionService.currentSelection.subscribe(selection => {
      if(selection.entity && selection.connection) {
        this.selection = selection;
        this.entityService.aboutEntity(this.selection.connection, this.selection.entity);
      }
    })
    this.entityService.aboutEntitySubject.subscribe(about => {
      this.aboutEntityData = about
    })
  }


}
