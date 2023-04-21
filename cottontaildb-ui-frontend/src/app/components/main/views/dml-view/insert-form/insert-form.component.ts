import { Component, OnInit } from '@angular/core';
import {FormBuilder, FormControl, FormGroup, Validators} from "@angular/forms";
import {SelectionService} from "../../../../../services/selection.service";
import {ColumnEntry, EntityService} from "../../../../../services/entity.service";

@Component({
  selector: 'app-insert-form',
  templateUrl: './insert-form.component.html',
  styleUrls: ['./insert-form.component.css']
})
export class InsertFormComponent implements OnInit {

  insertForm = new FormGroup({})
  aboutEntityData: any;
  selection: any;

  constructor(private fb: FormBuilder,
              private selectionService: SelectionService,
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

      if (this.insertForm) {
        Object.keys(this.insertForm.controls).forEach(it =>
          this.insertForm.removeControl(it)
        )
      }

      if (this.aboutEntityData != null) {
        this.aboutEntityData.forEach((item: { dbo: string; _class: string }) => {
          if (item._class === "COLUMN") {
            this.insertForm.addControl(item.dbo, new FormControl(null, [Validators.required]))
          }})
      }})
     }

  submitInsert() {
    /*Clear form text field upon submit*/
    let entries: Array<ColumnEntry> = []

    for (let [i, item] of Object.keys(this.insertForm.controls).entries()) {
      let value = Object.values(this.insertForm.controls).at(i) as FormControl
      entries.push(new ColumnEntry(item, value.value))
    }

    this.entityService.insertRow(this.selection.connection, this.selection.entity, entries)

    this.insertForm.reset();
  }


}

