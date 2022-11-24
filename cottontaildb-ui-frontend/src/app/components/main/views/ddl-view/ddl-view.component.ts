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
      if(selection.entity && selection.connection){
        this.selection = selection;
        this.entityService.aboutEntity(this.selection.connection, this.selection.entity);
      }
    })
    this.entityService.aboutEntitySubject.subscribe(about => this.aboutEntityData = about)
  }

  onDropIndex(port: number, dbo: string) {
    if(confirm("Are you sure you want to drop the index " + dbo + "?")){
      this.entityService.dropIndex(this.selection.connection, dbo).subscribe({
        next: () => {
          this.entityService.aboutEntity(this.selection.connection, this.selection.entity)
          this.selectionService.changeSelection(this.selection.connection, this.selection.entity)
        }})
      }}


  onCreateIndex(port: number, dbo: string) {
    this.dialog.open<CreateIndexFormComponent>(CreateIndexFormComponent, {
      width: 'fit-content',
      height: 'fit-content',
      data: {
        dbo, port
      }
    }).afterClosed().subscribe(
    () => this.entityService.aboutEntity(this.selection.connection.port, this.selection.entity)
    )
  }

}
