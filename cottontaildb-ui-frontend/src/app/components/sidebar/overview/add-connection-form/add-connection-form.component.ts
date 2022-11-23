import { Component, OnInit } from '@angular/core';
import {FormControl, FormGroup, Validators} from "@angular/forms";
import {ConnectionService} from "../../../../services/connection.service";
import {TreeDataService} from "../../../../services/tree-data.service";
import {MatDialogRef} from "@angular/material/dialog";

@Component({
  selector: 'app-add-connection-form',
  templateUrl: './add-connection-form.component.html',
  styleUrls: ['./add-connection-form.component.css']
})
export class AddConnectionFormComponent implements OnInit {

  constructor(private connectionService : ConnectionService,
              private treeDataService : TreeDataService,
              private dialogRef : MatDialogRef<AddConnectionFormComponent>) { }

  ngOnInit(): void {
  }

  connectionForm = new FormGroup({
    port: new FormControl<number|null>(null, [
      Validators.required,
      Validators.pattern("^[0-9]*$"),
    ])
  });

  submit() {
    let port = this.connectionForm.value.port;
    //let address = this.connectionForm.value.address
    /*Clear form text field upon submit*/
    this.connectionForm.reset();
    if (port != null) {
      this.connectionService.addConnection(port)
    }
    this.connectionService.connectionSubject.subscribe(value => console.log(value))
    this.dialogRef.close()
  }

}



