import { Component, OnInit } from '@angular/core';
import { FormControl, FormGroup, Validators} from "@angular/forms";
import {Schema, SchemaService} from "../../../../services/schema.service";
import {TreeDataService} from "../../../../services/tree-data.service";

@Component({
  selector: 'app-create-schema-form',
  templateUrl: './create-schema-form.component.html',
  styleUrls: ['./create-schema-form.component.css']
})
export class CreateSchemaFormComponent implements OnInit{

  constructor(private schemaService : SchemaService, private treeDataService : TreeDataService ) { }

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
    const schema: Schema = {
      /*We can be sure it name is not null because of our FormControl, hence [name!]*/
      name: name!
    }

    /*Clear form text field upon submit*/
    this.schemaForm.reset();

    this.schemaService.createSchema(schema).subscribe({
      next: () => this.treeDataService.fetchTreeData(),
      error: err => console.log(err)
    });
  }

}
