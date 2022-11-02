import {Component, OnInit} from '@angular/core';
import {EntityService} from "../../../../services/entity.service";
import {SelectionService} from "../../../../services/selection.service";
import {MatDialog} from "@angular/material/dialog";
import {CreateIndexFormComponent} from "./create-index-form/create-index-form.component";

@Component({
  selector: 'app-ddl-view',
  templateUrl: './ddl-view.component.html',
  styleUrls: ['./ddl-view.component.css']
})
export class DdlViewComponent implements OnInit {

  aboutEntityData: any;
  selection: any;


  constructor(private entityService : EntityService,
              private selectionService : SelectionService,
              private dialog : MatDialog,
  ) { }

  ngOnInit(): void {
    this.selectionService.currentSelection.subscribe(selection => {
      this.selection = selection;
      this.entityService.aboutEntity(this.selection.port, this.selection.entity);
    })
    this.entityService.aboutEntitySubject.subscribe(about => this.aboutEntityData = about)
  }

  onDropIndex(port: number, dbo: string) {
    if(confirm("Are you sure you want to drop the index " + dbo + "?")){
      this.entityService.dropIndex(port, dbo).subscribe()
      this.selectionService.changeSelection(this.selection.entity, this.selection.port)
      this.entityService.aboutEntity(this.selection.port, this.selection.entity)
    }
  }

  onCreateIndex(port: number, dbo: string) {
    this.dialog.open<CreateIndexFormComponent>(CreateIndexFormComponent, {
      width: 'fit-content',
      height: 'fit-content',
      data: {
        dbo, port
      }
    }).afterClosed().subscribe(
    result => this.entityService.aboutEntity(this.selection.port, this.selection.entity)
    )
  }

}
