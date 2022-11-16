import {Component, Input, OnInit} from '@angular/core';
import {FormBuilder, FormControl, FormGroupDirective, FormRecord, NG_VALUE_ACCESSOR} from "@angular/forms";
import {SelectionService} from "../../../../../services/selection.service";
import {EntityService} from "../../../../../services/entity.service";

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
export class DistanceFormComponent implements OnInit {

  @Input() index!: number;
  form: any
  selection: any
  aboutEntityData: any
  conditions: any;
  vectors: any;
  column: any;
  distances = ["MANHATTAN", "EUCLIDEAN", "SQUAREDEUCLIDEAN", "HAMMING", "COSINE", "CHISQUARED", "INNERPRODUCT", "HAVERSINE"];
  jsonStringify = JSON.stringify

  constructor(private fb: FormBuilder,
              private selectionService : SelectionService,
              private entityService: EntityService,
              private rootFormGroup: FormGroupDirective) {
  }

  ngOnInit(): void {

    this.form = this.rootFormGroup.control
    this.conditions = this.form.get("queryFunctions").at(this.index).get("conditions") as FormRecord

    this.selectionService.currentSelection.subscribe(selection => {
      if(selection.entity && selection.port) {
        this.selection = selection;
        this.entityService.aboutEntity(this.selection.port, this.selection.entity);
      }
    })

    this.entityService.aboutEntitySubject.subscribe(about => {
      this.aboutEntityData = about

      //if there are conditions left, reset them
      if(this.conditions){
        Object.keys(this.conditions.controls).forEach(it =>
          this.conditions.removeControl(it)
        )}

      this.conditions.addControl("column", new FormControl(""))
      this.conditions.addControl("vector", new FormControl())
      this.conditions.addControl("distance", new FormControl(""))
      this.conditions.addControl("name", new FormControl(""))

      this.conditions.get("column").valueChanges.subscribe((column: any) => {
        this.aboutEntityData.forEach((item: { dbo: string, lsize: number }) => {
          if (item.dbo.includes(column)){
            console.log(item.lsize)
            this.vectors = [Array(item.lsize).fill(0), Array(item.lsize).fill(1)]
          }
        }
        )
      })

    })




  }

}
