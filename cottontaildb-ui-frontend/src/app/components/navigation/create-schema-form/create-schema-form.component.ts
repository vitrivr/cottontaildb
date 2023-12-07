import {Component} from '@angular/core';
import {FormControl, FormGroup, Validators} from "@angular/forms";
import {MatDialogRef} from "@angular/material/dialog";

@Component({
  selector: 'app-create-schema-form',
  templateUrl: './create-schema-form.component.html'
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
  constructor(private dialogRef : MatDialogRef<CreateSchemaFormComponent>) {}

  /**
   * Saving of input (=> create schema)
   */
  public save() {
    this.dialogRef.close(this.schemaForm.value.name)
  }

  /**
   * Handles closing of dialog.
   */
  public abort() {
    this.dialogRef.close(null)
  }
}
