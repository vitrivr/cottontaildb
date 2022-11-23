import {Component, Input, OnInit} from "@angular/core";
import {FormGroupDirective, FormRecord} from "@angular/forms";
import {SelectionService} from "../../../../services/selection.service";
import {EntityService} from "../../../../services/entity.service";

@Component({
  template: ''
})
export abstract class AbstractQueryFormComponent implements OnInit {

  protected constructor(protected rootFormGroup: FormGroupDirective,
                        protected selectionService: SelectionService,
                        protected entityService: EntityService) { }

  @Input() index!: number;

  form: any
  conditions: any
  selection: any

  ngOnInit() {
    this.form = this.rootFormGroup.control
    this.conditions = this.form.get("queryFunctions").at(this.index).get("conditions") as FormRecord
  }
  remove() {
    this.form.get("queryFunctions").removeAt(this.index)
  }
  initSelect() {
    this.selectionService.currentSelection.subscribe(select => {
      if(select.entity != null && select.port != null) {
        this.selection = select;
        this.entityService.aboutEntity(this.selection.port, this.selection.entity);
      }
    })
  }
  resetConditions(){
    if(this.conditions){
      Object.keys(this.conditions.controls).forEach(it =>
        this.conditions.removeControl(it)
      )}
  }

}
