import { Component, OnInit } from '@angular/core';
import {MatDialog} from "@angular/material/dialog";
import {CreateSchemaFormComponent} from "./create-schema-form/create-schema-form.component";
import {AddConnectionFormComponent} from "./add-connection-form/add-connection-form.component";
import {ConnectionService} from "../../../services/connection.service";

@Component({
  selector: 'app-overview',
  templateUrl: './overview.component.html',
  styleUrls: ['./overview.component.css']
})
export class OverviewComponent implements OnInit {

  connections: any;

  constructor(private dialog:MatDialog, private connectionService: ConnectionService) { }

  ngOnInit(): void {
    this.connectionService.connectionSubject.subscribe(connections => this.connections = connections)
  }

  onCreateSchema() {
    this.dialog.open<CreateSchemaFormComponent>(CreateSchemaFormComponent, {
      width: 'fit-content',
      height: 'fit-content',
    });
  }

  onAddConnection() {
    this.dialog.open<AddConnectionFormComponent>(AddConnectionFormComponent, {
      width: 'fit-content',
      height: 'fit-content',
    });
  }
}
