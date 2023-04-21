import {Component, OnInit} from '@angular/core';
import {FormControl, FormGroupDirective, Validators} from "@angular/forms";
import {SelectionService} from "../../../../../services/selection.service";
import {EntityService} from "../../../../../services/entity.service";
import {AbstractQueryFormComponent} from "../AbstractQueryFormComponent";

@Component({
  selector: 'app-limit-form',
  templateUrl: './limit-form.component.html',
  styleUrls: ['./limit-form.component.css']
})
export class LimitFormComponent extends AbstractQueryFormComponent implements OnInit {

  constructor(selectionService: SelectionService,
              entityService: EntityService,
              rootFormGroup: FormGroupDirective) {
    super(rootFormGroup, selectionService, entityService);
  }

  override ngOnInit(): void {
    super.ngOnInit()
    this.initSelect()
    this.resetConditions()
    this.conditions.addControl("limit", new FormControl("",[Validators.required, Validators.pattern("^[0-9]*$")]))
  }

  numberOnly($event: KeyboardEvent): boolean {
    const key = ($event.key)
    return (key >= '0' && key <= '9') || key == "Backspace";
  }

}
