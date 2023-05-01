import {Component} from '@angular/core';
import {FormControl, FormGroup, Validators} from "@angular/forms";
import {Connection} from "../../../../../openapi";
import {MatDialogRef} from "@angular/material/dialog";

@Component({
  selector: 'app-add-connection-form',
  templateUrl: './add-connection-form.component.html'
})
export class AddConnectionFormComponent {

  /** The {@link FormGroup} used to enter the connection details. */
  public connectionForm = new FormGroup({
    host: new FormControl<string|null>(null, [
      Validators.required,
    ]),
    port: new FormControl<number|null>(null, [
      Validators.required,
      Validators.min(1025),
      Validators.max(65535)
    ])
  });

  /**
   * Default constructor.
   *
   * @param dialogRef The {@link MatDialogRef} to use.
   */
  constructor(private dialogRef : MatDialogRef<AddConnectionFormComponent>) { }

  /**
   * Handles connecting using the provided connection details
   */
  public connect() {
    if (this.connectionForm.valid) {
      this.dialogRef.close({host: this.connectionForm.value.host, port: this.connectionForm.value.port} as Connection)
    }
  }

  /**
   * Handles closing of dialog.
   */
  public abort() {
    this.dialogRef.close(null)
  }
}



