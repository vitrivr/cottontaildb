import { Component, OnInit } from '@angular/core';
import {SelectionService} from "../../../../../services/selection.service";
import {EntityService} from "../../../../../services/entity.service";
import {FormGroupDirective} from "@angular/forms";
import {AbstractQueryFormComponent} from "../AbstractQueryFormComponent";

@Component({
  selector: 'app-use-index-type-form',
  templateUrl: './use-index-type-form.component.html',
  styleUrls: ['./use-index-type-form.component.css']
})
export class UseIndexTypeFormComponent extends AbstractQueryFormComponent implements OnInit {

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
