import {Component, OnInit} from '@angular/core';
import {TreeDataService} from "../../../../services/tree-data.service";
import {FormControl, FormGroup, Validators} from "@angular/forms";
import {EntityService} from "../../../../services/entity.service";
import {MatStepper} from "@angular/material/stepper";

export interface ColumnDef {
  name : string;
  type: string;
  nullable: boolean;
}

@Component({
  selector: 'app-create-entity-form',
  templateUrl: './create-entity-form.component.html',
  styleUrls: ['./create-entity-form.component.css']
})


export class CreateEntityFormComponent implements OnInit {

  schemaName: string;
  columnData: any;
  entityNameSet: boolean;
  displayedColumns: string[] = ['name', 'type', 'nullable'];
  schemaNameSet: boolean;

  constructor(private entityService : EntityService, private treeDataService : TreeDataService ) {
    this.schemaName = "";
    this.columnData = [];
    this.entityNameSet = false;
    this.schemaNameSet = false;
  }

  ngOnInit(): void {
  }

  entityForm = new FormGroup({
    name: new FormControl('', [Validators.required, Validators.minLength(3)])
  });

  columnForm = new FormGroup({
    name: new FormControl('', [Validators.required]),
    type: new FormControl('', [Validators.required]),
    nullable: new FormControl('', [Validators.required]),
  });

  types: string[] = ["BOOLEAN", "BYTE", "SHORT", "INTEGER",
    "LONG", "FLOAT", "DOUBLE", "DATE", "STRING",
    "COMPLEX32", "COMPLEX64", "DOUBLE_VEC", "FLOAT_VEC",
    "LONG_VEC", "INT_VEC", "BOOL_VEC", "COMPLEX32_VEC",
    "COMPLEX64_VEC", "BYTESTRING", "UNRECOGNIZED"];



  submitEntityName() {
    this.entityNameSet = true;
  }

  submitColumn() : void {

    this.columnData.push(this.columnForm.value);
    this.columnForm.reset();
  }


  removeItem(element : ColumnDef) {
    this.columnData.forEach( (value : ColumnDef, index: number) => {
      if (value == element){ this.columnData.splice(index,1) }
    });
  }

  onCreateEntity() {
    console.log(this.columnData)
    let entityName = this.entityForm.value.name
    console.log(entityName)
    this.entityService.createEntity(this.schemaName, entityName!, this.columnData).subscribe({
      next: () => this.treeDataService.fetchTreeData(),
      error: err => console.log(err)
    });
    console.log("fetch")

  }

  onReset(){
    this.schemaName = "";
    this.entityForm.reset()
    this.columnForm.reset()
    this.entityNameSet = false
    this.columnData.forEach( (value : ColumnDef, index: number) => {
      this.columnData.splice(index,1)
    });

  }

  setSchemaName(stepper: MatStepper, name : string) {
    this.schemaName = name
    this.schemaNameSet = true
    stepper.next()
  }


}
