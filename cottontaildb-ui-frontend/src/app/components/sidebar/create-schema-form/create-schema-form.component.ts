import {Component, OnInit, Inject} from '@angular/core';
import { FormControl, FormGroup, Validators} from "@angular/forms";
import {MatDialogRef} from "@angular/material/dialog";

@Component({
  selector: 'app-create-schema-form',
  templateUrl: './create-schema-form.component.html',
  styleUrls: ['./create-schema-form.component.css']
})
export class CreateSchemaFormComponent {

  /** The {@link FormGroup} used to enter the schema name. */
  public readonly schemaForm = new FormGroup({
    name: new FormControl('', [Validators.required, Validators.minLength(3)])
  });


  /**
   * Default constructor.
   *
   * @param dialogRef
   */
  constructor(private dialogRef : MatDialogRef<CreateSchemaFormComponent>) {

  }

  /**
   * Handles form submission.
   */
  public submit() {
    this.dialogRef.close(this.schemaForm.value.name)
  }
}
