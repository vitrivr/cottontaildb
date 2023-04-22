import {Component, OnInit} from '@angular/core';
import {FormGroupDirective} from "@angular/forms";
import {AbstractQueryFormComponent} from "../AbstractQueryFormComponent";
import {SelectionService} from "../../../../../services/selection.service";
import {EntityService} from "../../../../../services/entity.service";

@Component({
  selector: 'app-count-form',
  templateUrl: './count-form.component.html',
  styleUrls: ['./count-form.component.css']
})
export class CountFormComponent extends AbstractQueryFormComponent implements OnInit {

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
