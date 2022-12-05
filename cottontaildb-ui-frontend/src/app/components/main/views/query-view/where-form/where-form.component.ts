import {Component, OnInit} from '@angular/core';
import {FormControl, FormGroupDirective} from "@angular/forms";
import {SelectionService} from "../../../../../services/selection.service";
import {EntityService} from "../../../../../services/entity.service";
import {AbstractQueryFormComponent} from "../AbstractQueryFormComponent";

@Component({
  selector: 'app-where-form',
  templateUrl: './where-form.component.html',
  styleUrls: ['./where-form.component.css']
})
export class WhereFormComponent extends AbstractQueryFormComponent implements OnInit {

  aboutEntityData: any
  operators: Array<string> =
    ["==","!=",">","<",">=","<=","NOT IN","NOT LIKE","IS NULL","IS NOT NULL"];


  constructor(selectionService: SelectionService,
              entityService: EntityService,
              rootFormGroup: FormGroupDirective) {
    super(rootFormGroup, selectionService, entityService);
  }

  override ngOnInit(): void {
    super.ngOnInit()
    this.initSelect()

    this.entityService.aboutEntitySubject.subscribe(about => {
      this.aboutEntityData = about
      this.resetConditions()

      if(this.aboutEntityData != null){
        this.conditions.addControl("column", new FormControl(""))
        this.conditions.addControl("operator", new FormControl(""))
        this.conditions.addControl("value", new FormControl(""))
      }
    })
  }
}
