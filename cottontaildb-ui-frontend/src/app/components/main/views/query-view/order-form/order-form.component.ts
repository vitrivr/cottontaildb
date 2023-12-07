import {Component, OnInit} from '@angular/core';
import {FormBuilder, FormControl, FormGroupDirective} from "@angular/forms";
import {SelectionService} from "../../../../../services/selection.service";
import {EntityService} from "../../../../../services/entity.service";
import {AbstractQueryFormComponent} from "../AbstractQueryFormComponent";

@Component({
  selector: 'app-order-form',
  templateUrl: './order-form.component.html',
  styleUrls: ['./order-form.component.css']
})
export class OrderFormComponent extends AbstractQueryFormComponent implements OnInit {

  aboutEntityData: any

  constructor( fb: FormBuilder,
               selectionService: SelectionService,
               entityService: EntityService,
               rootFormGroup: FormGroupDirective) {
    super(rootFormGroup, selectionService, entityService);
  }

  override ngOnInit(): void {

    super.ngOnInit();
    this.initSelect()

    this.entityService.aboutEntitySubject.subscribe(about => {
      this.aboutEntityData = about
      this.resetConditions()

      if(this.aboutEntityData != null){
        this.conditions.addControl("column", new FormControl(""))
        this.conditions.addControl("direction", new FormControl(""))
      }
    })
  }
}
