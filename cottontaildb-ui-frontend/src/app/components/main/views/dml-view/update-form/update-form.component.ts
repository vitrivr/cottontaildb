import { Component, OnInit } from '@angular/core';
import {FormBuilder, FormControl, FormGroup, Validators} from "@angular/forms";
import {SelectionService} from "../../../../../services/selection.service";
import {ColumnEntry, EntityService} from "../../../../../services/entity.service";

@Component({
  selector: 'app-update-form',
  templateUrl: './update-form.component.html',
  styleUrls: ['./update-form.component.css']
})
export class UpdateFormComponent implements OnInit {

  operators: Array<string> = ["=","==","!=","!==",">","<",">=","<=","NOT IN","NOT LIKE","IS NULL","IS NOT NULL"];

  aboutEntityData: any;
  selection: any;

  updateForm = new FormGroup({
    column: new FormControl("", [Validators.required]),
    operator: new FormControl("", [Validators.required]),
    value: new FormControl("", [Validators.required]),
    updates: new FormGroup({})
  })
  updates = this.updateForm.get("updates") as FormGroup

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
      if (this.updateForm) {
        Object.keys(this.updateForm.controls).forEach(it =>
          this.updates.removeControl(it)
        )
      }

      if (this.aboutEntityData != null) {
        this.aboutEntityData.forEach((item: { dbo: string; _class: string }) => {
          if (item._class === "COLUMN") {
            this.updates.addControl(item.dbo, new FormControl(null, ))
          }})
      }
    })
  }

  submitUpdate() {
    let column = this.updateForm.value.column;
    let operator = this.updateForm.value.operator;
    let value = this.updateForm.value.value;

    let type = null;

    this.aboutEntityData.forEach((it: { dbo: string, type: string }) =>{
        if (it.dbo == column){
          type = it.type as string
          console.log(type)
        }
      }
    )

    let updateValues = new Array<ColumnEntry>()

    for (let [i, item] of Object.keys(this.updates.controls).entries()) {
      let value = Object.values(this.updates.controls).at(i) as FormControl
      if(value.value != null && value.value != ""){
        updateValues.push(new ColumnEntry(item, value.value))
      }
    }

    console.log(updateValues)

    if (column != null && operator != null && value != null && type != null) {
      console.log("handing over to service")
      this.entityService.updateRow(this.selection.connection, this.selection.entity, column, operator, value, type, updateValues)
    }

  }




}
