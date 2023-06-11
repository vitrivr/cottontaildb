import {Component, OnInit} from '@angular/core';
import {FormControl, FormGroupDirective, NG_VALUE_ACCESSOR, Validators} from "@angular/forms";
import {SelectionService} from "../../../../../services/selection.service";
import {EntityService} from "../../../../../services/entity.service";
import {AbstractQueryFormComponent} from "../AbstractQueryFormComponent";

@Component({
  selector: 'app-distance-form',
  templateUrl: './distance-form.component.html',
  styleUrls: ['./distance-form.component.css'],
  providers: [{
    provide: NG_VALUE_ACCESSOR,
    useExisting: DistanceFormComponent,
    multi: true
  }]
})
export class DistanceFormComponent extends AbstractQueryFormComponent implements OnInit {

  aboutEntityData: any
  standardVectors: any;
  column: any;
  distances = ["MANHATTAN", "EUCLIDEAN", "SQUAREDEUCLIDEAN", "HAMMING", "COSINE", "CHISQUARED", "INNERPRODUCT", "HAVERSINE"];
  jsonStringify = JSON.stringify

  constructor(selectionService: SelectionService,
              entityService: EntityService,
              rootFormGroup: FormGroupDirective) {
    super(rootFormGroup, selectionService, entityService);
  }

  override ngOnInit(): void {
    super.ngOnInit()
    this.initSelect()

    //change every time a new entity is selected
    this.entityService.aboutEntitySubject.subscribe(about => {
      this.aboutEntityData = about
      this.resetConditions()

      for(let controlName of ["column","vector", "vectorType", "distance", "name"]){
        this.conditions.addControl(controlName, new FormControl("", [Validators.required]))
      }

      this.conditions.get("column").valueChanges.subscribe((column: any) => {
        this.aboutEntityData.forEach((item: { dbo: string, lsize: number, type: string }) => {
            if (item.dbo.includes(column)) {
              console.log(item.lsize)
              this.standardVectors = [Array(item.lsize).fill(0), Array(item.lsize).fill(1)]
              this.conditions.get("vectorType").setValue(item.type)
            }
          }
        )
      })

    })
  }
}
