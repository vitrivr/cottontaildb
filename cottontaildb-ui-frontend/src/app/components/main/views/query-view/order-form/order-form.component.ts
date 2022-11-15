import {Component, Input, OnInit} from '@angular/core';
import {FormBuilder, FormControl, FormGroupDirective, FormRecord} from "@angular/forms";
import {SelectionService} from "../../../../../services/selection.service";
import {EntityService} from "../../../../../services/entity.service";

@Component({
  selector: 'app-order-form',
  templateUrl: './order-form.component.html',
  styleUrls: ['./order-form.component.css']
})
export class OrderFormComponent implements OnInit {

  @Input() index!: number;

  conditions: any;
  form: any
  aboutEntityData: any
  selection: any

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
        this.conditions.addControl("direction", new FormControl(""))
      }
    })


  }





}
