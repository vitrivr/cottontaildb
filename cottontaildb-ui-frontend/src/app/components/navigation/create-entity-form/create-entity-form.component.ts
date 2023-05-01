import {Component, Inject, OnInit} from '@angular/core';
import {TreeDataService} from "../../../services/tree-data.service";
import {FormControl, FormGroup, Validators} from "@angular/forms";
import {EntityService} from "../../../services/entity.service";
import {ColumnDefinition} from "../../../model/ColumnDefinition";
import {BehaviorSubject} from "rxjs";
import {Connection} from "../../../../../openapi";
import {MAT_DIALOG_DATA, MatDialogRef} from "@angular/material/dialog";
import {MatSnackBar} from "@angular/material/snack-bar";


@Component({
  selector: 'app-create-entity-form',
  templateUrl: './create-entity-form.component.html',
  styleUrls: ['./create-entity-form.component.css']
})


export class CreateEntityFormComponent implements OnInit {

  schemaName: string;
  columnData: any;
  columnDataSource: any;
  connection: any;

  constructor(
    @Inject(MAT_DIALOG_DATA) public data: {name: string, connection: Connection},
    private entityService : EntityService,
    private treeDataService : TreeDataService,
    private dialogRef : MatDialogRef<CreateEntityFormComponent>,
    private snackBar: MatSnackBar)
  {
      this.schemaName = data.name
      this.connection = data.connection
      this.columnDataSource = new BehaviorSubject([]);
      this.columnData = []
  }

  ngOnInit(): void {
  }

  entityForm = new FormGroup({
    name: new FormControl('', [Validators.required])
  });

  columnForm = new FormGroup({
    name: new FormControl('', [Validators.required]),
    type: new FormControl('', [Validators.required]),
    size: new FormControl('', [Validators.required]),
    nullable: new FormControl('', [Validators.required]),
  });

  types: string[] = ["BOOLEAN", "BYTE", "SHORT", "INTEGER",
    "LONG", "FLOAT", "DOUBLE", "DATE", "STRING",
    "COMPLEX32", "COMPLEX64", "DOUBLE_VEC", "FLOAT_VEC",
    "LONG_VEC", "INT_VEC", "BOOL_VEC", "COMPLEX32_VEC",
    "COMPLEX64_VEC", "BYTESTRING", "UNRECOGNIZED"];

  submitColumn() : void {
    this.columnData.push(this.columnForm.value);
    this.columnForm.reset()
    this.columnDataSource.next(this.columnData)
  }


  removeItem(element : ColumnDefinition) {
    this.columnData.forEach( (value : ColumnDefinition, index: number) => {
      if (value == element){ this.columnData.splice(index,1) }
    });
    this.columnDataSource.next(this.columnData)
  }

  onCreateEntity() {
    console.log(this.columnData)
    let entityName = this.entityForm.value.name
    console.log(entityName)
    console.log(this.connection)
    this.entityService.createEntity(this.connection, this.schemaName, entityName!, this.columnData).subscribe({
      next: () => this.treeDataService.fetchTreeData(this.connection),
      error: err => console.log(err)
    });
    this.dialogRef.close()
    this.snackBar.open( "created entity "+entityName+" successfully", "ok", {duration:2000})
  }

  onReset(){
    this.entityForm.reset()
    this.columnForm.reset()
    this.columnData.forEach( (value : ColumnDefinition, index: number) => {
      this.columnData.splice(index,1)
    });

  }
}
