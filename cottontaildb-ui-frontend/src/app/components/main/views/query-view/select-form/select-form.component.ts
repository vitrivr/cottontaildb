import {Component, Input, OnInit} from '@angular/core';
import {
  FormBuilder, FormControl,
  FormGroupDirective, FormRecord,
  NG_VALUE_ACCESSOR
} from "@angular/forms";
import {SelectionService} from "../../../../../services/selection.service";
import {EntityService} from "../../../../../services/entity.service";


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

export class SelectFormComponent implements OnInit {

  @Input() index!: number;

  form: any
  selection: any
  aboutEntityData: any
  conditions: any;

  constructor(private fb: FormBuilder,
              private selectionService: SelectionService,
              private entityService: EntityService,
              private rootFormGroup: FormGroupDirective) { }

  ngOnInit(): void {

    this.selectionService.currentSelection.subscribe(selection => {
      this.selection = selection;
      this.entityService.aboutEntity(this.selection.port, this.selection.entity);
    })

    this.entityService.aboutEntitySubject.subscribe(about => {
      this.aboutEntityData = about

      //if there are conditions left, reset them
      if(this.conditions){
        Object.keys(this.conditions.controls).forEach(it =>
        this.conditions.removeControl(it)
      )}

      if(this.aboutEntityData != null){
        this.aboutEntityData.forEach((item: { dbo: string; _class: string }) => {
          if(item._class === "COLUMN"){
            this.conditions.addControl(item.dbo, new FormControl(false))
          }
        })
      }
    })

    this.form = this.rootFormGroup.control
    this.conditions = this.form.get("queryFunctions").at(this.index).get("conditions") as FormRecord
  }

}
