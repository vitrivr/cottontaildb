import { Component, OnInit } from '@angular/core';
import {EntityService} from "../../../services/entity.service";
import {SelectionService} from "../../../services/selection.service";

@Component({
  selector: 'app-ddl-view',
  templateUrl: './ddl-view.component.html',
  styleUrls: ['./ddl-view.component.css']
})
export class DdlViewComponent implements OnInit {

  aboutEntityData: any;
  selection: any;

  constructor(private entityService : EntityService,
              private selectionService : SelectionService) { }

  ngOnInit(): void {
    this.selectionService.currentSelection.subscribe(selection => {
      this.selection = selection;
      this.entityService.aboutEntity(this.selection);
    })
    this.entityService.aboutEntitySubject.subscribe(about => this.aboutEntityData = about)
  }

  onDropIndex(dbo: string) {
    this.entityService.dropIndex(dbo).subscribe()
    this.selectionService.changeSelection(this.selection)
  }

  onCreateIndex(dbo: string) {
  }

}
