import { Component, OnInit } from '@angular/core';
import {AbstractQueryFormComponent} from "../AbstractQueryFormComponent";
import {SelectionService} from "../../../../../services/selection.service";
import {EntityService} from "../../../../../services/entity.service";
import {FormGroupDirective} from "@angular/forms";

@Component({
  selector: 'app-disallow-parallelism-form',
  templateUrl: './disallow-parallelism-form.component.html',
  styleUrls: ['./disallow-parallelism-form.component.css']
})
export class DisallowParallelismFormComponent extends AbstractQueryFormComponent implements OnInit {

  constructor(selectionService: SelectionService,
              entityService: EntityService,
              rootFormGroup: FormGroupDirective) {
    super(rootFormGroup, selectionService, entityService);
  }

  override ngOnInit(): void {
    super.ngOnInit()
    this.initSelect()
  }

}
