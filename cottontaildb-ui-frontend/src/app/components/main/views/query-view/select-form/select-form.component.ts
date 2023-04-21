import {Component, OnInit} from '@angular/core';
import {FormControl, FormGroupDirective, NG_VALUE_ACCESSOR} from "@angular/forms";
import {SelectionService} from "../../../../../services/selection.service";
import {EntityService} from "../../../../../services/entity.service";
import { AbstractQueryFormComponent} from "../AbstractQueryFormComponent";


@Component({
  selector: 'app-select-form',
  templateUrl: './select-form.component.html',
  styleUrls: ['./select-form.component.css'],
  providers: [{
      provide: NG_VALUE_ACCESSOR,
      useExisting: SelectFormComponent,
      multi: true
    }]
})

export class SelectFormComponent extends AbstractQueryFormComponent implements OnInit {


  override selection: any
  aboutEntityData: any

  constructor(selectionService: SelectionService,
              entityService: EntityService,
              rootFormGroup: FormGroupDirective) {
    super(rootFormGroup, selectionService, entityService);
  }

  override ngOnInit() {

    super.ngOnInit();
    this.initSelect()

    this.entityService.aboutEntitySubject.subscribe(about => {
      this.aboutEntityData = about
      this.resetConditions()

      if(this.aboutEntityData != null){
        this.aboutEntityData.forEach((item: { dbo: string; _class: string }) => {
          if(item._class === "COLUMN"){
            this.conditions.addControl(item.dbo, new FormControl(false))
          }
        })
      }
    })
  }

}
