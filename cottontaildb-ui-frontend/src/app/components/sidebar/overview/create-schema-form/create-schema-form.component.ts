import {Component, Input, OnInit} from '@angular/core';
import { FormControl, FormGroup, Validators} from "@angular/forms";
import {Schema, SchemaService} from "../../../../services/schema.service";
import {TreeDataService} from "../../../../services/tree-data.service";
import {MatDialogRef} from "@angular/material/dialog";

@Component({
  selector: 'app-create-schema-form',
  templateUrl: './create-schema-form.component.html',
  styleUrls: ['./create-schema-form.component.css']
})
export class CreateSchemaFormComponent implements OnInit{

  constructor(private schemaService : SchemaService,
              private treeDataService : TreeDataService,
              private dialogRef : MatDialogRef<CreateSchemaFormComponent>
  ) { }

  ngOnInit(): void {
  }
  @Input() connection : any;

  schemaForm = new FormGroup({
    name: new FormControl('', [
      Validators.required,
      Validators.minLength(3),
    ])
  });


  submit() {
    let name = this.schemaForm.value.name;
    const schema: Schema = {
      /*We can be sure it name is not null because of our FormControl, hence [name!]*/
      name: name!
    }
    /*Clear form text field upon submit*/
    this.schemaForm.reset();
    this.schemaService.createSchema(this.connection, schema).subscribe({
      next: () => this.treeDataService.fetchTreeData(this.connection),
      error: err => console.log(err)
    });
    this.dialogRef.close()
  }

}
