import { Component, OnInit } from '@angular/core';
import {MatDialog} from "@angular/material/dialog";
import {CreateSchemaFormComponent} from "./create-schema-form/create-schema-form.component";
import {AddConnectionFormComponent} from "./add-connection-form/add-connection-form.component";
import {Connection, ConnectionService} from "../../../services/connection.service";

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

  onCreateSchema(connection: Connection) {
    this.dialog.open<CreateSchemaFormComponent>(CreateSchemaFormComponent, {
      width: 'fit-content',
      height: 'fit-content',
      data: {
        connection: connection
      }
    });
  }

  onAddConnection() {
    this.dialog.open<AddConnectionFormComponent>(AddConnectionFormComponent, {
      width: 'fit-content',
      height: 'fit-content',
    });
  }

  connectionName = this.connectionService.connectionName

  onRemoveConnection(connection: any) {
    this.connectionService.removeConnection(connection)
  }
}
