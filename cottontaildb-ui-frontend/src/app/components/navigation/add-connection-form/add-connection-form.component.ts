import {Component} from '@angular/core';
import {FormControl, FormGroup, Validators} from "@angular/forms";
import {MatLegacyDialogRef as MatDialogRef} from "@angular/material/legacy-dialog";
import {Connection} from "../../../../../openapi";

@Component({
  selector: 'app-add-connection-form',
  templateUrl: './add-connection-form.component.html',
  styleUrls: ['./add-connection-form.component.css']
})
export class AddConnectionFormComponent {

  /** The {@link FormGroup} used to enter the connection details. */
  public connectionForm = new FormGroup({
    port: new FormControl<number|null>(null, [
      Validators.required,
      Validators.pattern("^[0-9]*$"),
    ]),
    address: new FormControl<string|null>(null, [
      Validators.required,
    ])
  });

  /**
   * Default constructor.
   *
   * @param dialogRef The {@link MatDialogRef} to use.
   */
  constructor(private dialogRef : MatDialogRef<AddConnectionFormComponent>) { }

  /**
   *
   */
  submit() {
    if (this.connectionForm.valid) {
      this.dialogRef.close({host: this.connectionForm.value.address, port: this.connectionForm.value.port} as Connection)
    }
  }
}



