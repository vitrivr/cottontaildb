import {Component, OnInit, Inject} from '@angular/core';
import { FormControl, FormGroup, Validators} from "@angular/forms";
import {SchemaService} from "../../../../services/schema.service";
import {TreeDataService} from "../../../../services/tree-data.service";
import {MatDialogRef} from "@angular/material/dialog";
import {MAT_DIALOG_DATA} from '@angular/material/dialog';
import {Connection} from "../../../../services/connection.service";


@Component({
  selector: 'app-create-schema-form',
  templateUrl: './create-schema-form.component.html',
  styleUrls: ['./create-schema-form.component.css']
})
export class CreateSchemaFormComponent implements OnInit{

  connection: any

  constructor(@Inject(MAT_DIALOG_DATA) public data: {connection: Connection},
              private schemaService : SchemaService,
              private treeDataService : TreeDataService,
              private dialogRef : MatDialogRef<CreateSchemaFormComponent>,
  ) { this.connection = data.connection }

  ngOnInit(): void {
  }


  schemaForm = new FormGroup({
    name: new FormControl('', [
      Validators.required,
      Validators.minLength(3),
    ])
  });


  submit() {
    let name = this.schemaForm.value.name;
    /*Clear form text field upon submit*/
    this.schemaForm.reset();
    if(name != null){
      this.schemaService.createSchema(this.connection, name).subscribe({
        next: () => this.treeDataService.fetchTreeData(this.connection),
        error: err => console.log(err)
      });
    }
    this.dialogRef.close()
  }

}
