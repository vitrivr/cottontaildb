import { Component, OnInit } from '@angular/core';
import {FormBuilder, FormControl, FormGroup, Validators} from "@angular/forms";
import {SelectionService} from "../../../../services/selection.service";
import {EntityService} from "../../../../services/entity.service";


@Component({
  selector: 'app-dml-view',
  templateUrl: './dml-view.component.html',
  styleUrls: ['./dml-view.component.css']
})
export class DmlViewComponent implements OnInit {

  operators: Array<string> = ["=","==","!=","!==",">","<",">=","<=","NOT IN","NOT LIKE","IS NULL","IS NOT NULL"];

  deleteForm = new FormGroup({
    column: new FormControl("", [Validators.required]),
    operator: new FormControl("", [Validators.required]),
    value: new FormControl("", [Validators.required])
  })
  aboutEntityData: any;
  selection: any;
  deleteData: any;

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
    })
  }

  submitDelete() {
    let column = this.deleteForm.value.column;
    let operator = this.deleteForm.value.operator;
    let value = this.deleteForm.value.value;
    let type = null;

    this.aboutEntityData.forEach((it: { dbo: string, type: string }) =>{
      if (it.dbo == column){
        type = it.type as string
        console.log(type)
        }
      }
    )

    /*Clear form text field upon submit*/
    this.deleteForm.reset();
    if (column != null && operator != null && value != null && type != null) {
      this.entityService.deleteRow(this.selection.connection, this.selection.entity, column, operator, value, type)
      this.entityService.deleteSubject.subscribe(
        {next: msg => {
          this.deleteData = msg
          },
        error: err => console.log(err.status)
        })
    }

  }


}
