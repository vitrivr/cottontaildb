import {Component, Input, OnInit} from '@angular/core';
import {TreeDataService} from "../../../../services/tree-data.service";
import {FormControl, FormGroup, Validators} from "@angular/forms";
import {EntityService} from "../../../../services/entity.service";
import {MAT_DIALOG_DATA} from '@angular/material/dialog';
import { Inject } from '@angular/core';
import {MatDialogRef} from "@angular/material/dialog";
import {MatSnackBar} from "@angular/material/snack-bar";
import {ColumnDefinition} from "../../../../interfaces/ColumnDefinition";


@Component({
  selector: 'app-create-entity-form',
  templateUrl: './create-entity-form.component.html',
  styleUrls: ['./create-entity-form.component.css']
})


export class CreateEntityFormComponent implements OnInit {

  schemaName: string;
  columnData: any;
  entityNameSet: boolean;


  constructor(
    @Inject(MAT_DIALOG_DATA) public data: {name: string},
    private entityService : EntityService,
    private treeDataService : TreeDataService,
    private dialogRef : MatDialogRef<CreateEntityFormComponent>,
    private snackBar: MatSnackBar)
  {
      this.schemaName = data.name
      this.columnData = []
      this.entityNameSet = false
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

  @Input() port = 0;

  submitEntityName() {
    this.entityNameSet = true;
  }

  submitColumn() : void {
    this.columnData.push(this.columnForm.value);
    this.columnForm.reset();
  }


  removeItem(element : ColumnDefinition) {
    this.columnData.forEach( (value : ColumnDefinition, index: number) => {
      if (value == element){ this.columnData.splice(index,1) }
    });
  }

  onCreateEntity() {
    console.log(this.columnData)
    let entityName = this.entityForm.value.name
    console.log(entityName)
    this.entityService.createEntity(this.port, this.schemaName, entityName!, this.columnData).subscribe({
      next: () => this.treeDataService.fetchTreeData(this.port),
      error: err => console.log(err)
    });
    this.dialogRef.close()
    this.snackBar.open( "created entity "+entityName+" successfully", "ok", {duration:2000})
  }

  onReset(){
    this.entityForm.reset()
    this.columnForm.reset()
    this.entityNameSet = false
    this.columnData.forEach( (value : ColumnDefinition, index: number) => {
      this.columnData.splice(index,1)
    });

  }




}
