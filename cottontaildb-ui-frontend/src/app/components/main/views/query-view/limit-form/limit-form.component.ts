import {Component, Input, OnInit} from '@angular/core';
import {FormBuilder, FormControl, FormGroupDirective, FormRecord} from "@angular/forms";
import {SelectionService} from "../../../../../services/selection.service";
import {EntityService} from "../../../../../services/entity.service";

@Component({
  selector: 'app-limit-form',
  templateUrl: './limit-form.component.html',
  styleUrls: ['./limit-form.component.css']
})
export class LimitFormComponent implements OnInit {


  @Input() index!: number;

  conditions: any;
  form: any

  constructor(private fb: FormBuilder,
              private selectionService: SelectionService,
              private entityService: EntityService,
              private rootFormGroup: FormGroupDirective) { }

  ngOnInit(): void {

    this.form = this.rootFormGroup.control
    this.conditions = this.form.get("queryFunctions").at(this.index).get("conditions") as FormRecord

    //if there are conditions left, reset them
    if (this.conditions) {
      Object.keys(this.conditions.controls).forEach(it =>
        this.conditions.removeControl(it)
      )
    }

    this.conditions.addControl("limit", new FormControl())

  }
}
