import {Component, Input, OnInit} from '@angular/core';
import {FormBuilder, FormControl, FormGroupDirective, FormRecord} from "@angular/forms";
import {SelectionService} from "../../../../../services/selection.service";
import {EntityService} from "../../../../../services/entity.service";

@Component({
  selector: 'app-where-form',
  templateUrl: './where-form.component.html',
  styleUrls: ['./where-form.component.css']
})
export class WhereFormComponent implements OnInit {

  @Input() index!: number;

  form: any
  selection: any
  aboutEntityData: any
  conditions: any;
  operators: Array<string> = ["=","==","!=","!==",">","<",">=","<=","NOT IN","NOT LIKE","IS NULL","IS NOT NULL"];


  constructor(private fb: FormBuilder,
              private selectionService: SelectionService,
              private entityService: EntityService,
              private rootFormGroup: FormGroupDirective) { }

  ngOnInit(): void {

    this.form = this.rootFormGroup.control
    this.conditions = this.form.get("queryFunctions").at(this.index).get("conditions") as FormRecord

    this.selectionService.currentSelection.subscribe(selection => {
      if(selection.entity && selection.port) {
        this.selection = selection;
        this.entityService.aboutEntity(this.selection.port, this.selection.entity);
      }
    })

    this.entityService.aboutEntitySubject.subscribe(about => {
      this.aboutEntityData = about

      //if there are conditions left, reset them
      if(this.conditions){
        Object.keys(this.conditions.controls).forEach(it =>
          this.conditions.removeControl(it)
        )}

      if(this.aboutEntityData != null){
        this.conditions.addControl("column", new FormControl(""))
        this.conditions.addControl("operator", new FormControl(""))
        this.conditions.addControl("value", new FormControl(""))
      }
    })
  }
  remove() {
    this.form.get("queryFunctions").removeAt(this.index)
  }

}
