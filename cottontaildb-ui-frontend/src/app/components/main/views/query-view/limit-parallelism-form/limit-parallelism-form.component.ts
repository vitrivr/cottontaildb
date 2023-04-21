import { Component, OnInit } from '@angular/core';
import {AbstractQueryFormComponent} from "../AbstractQueryFormComponent";
import {FormGroupDirective} from "@angular/forms";
import {SelectionService} from "../../../../../services/selection.service";
import {EntityService} from "../../../../../services/entity.service";

@Component({
  selector: 'app-limit-parallelism-form',
  templateUrl: './limit-parallelism-form.component.html',
  styleUrls: ['./limit-parallelism-form.component.css']
})
export class LimitParallelismFormComponent extends AbstractQueryFormComponent implements OnInit {

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
