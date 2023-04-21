import { Injectable } from '@angular/core';
import {BehaviorSubject} from "rxjs";
import {MatSnackBar} from "@angular/material/snack-bar";
import {HttpParams} from "@angular/common/http";

export class Connection {
  address: string;
  port: number;
  constructor(address: string, port: number) {
    this.address = address
    this.port = port
  }
}

@Injectable({
  providedIn: 'root'
})
export class ConnectionService {

  public apiURL = 'http://localhost:7070/'

  connectionSubject = new BehaviorSubject<Set<Connection>>(new Set<Connection>);

  constructor(private snackBar: MatSnackBar) {
  }

  addConnection(address: string, port: number) {
    let uniqueConnection = true
    this.connectionSubject.getValue().forEach(connection => {
      if(connection.address == address && connection.port == port){
        uniqueConnection = false
        this.snackBar.open("connection already present", "ok", {duration:2000})
      }
    })
    if(uniqueConnection) {
      let value = this.connectionSubject.getValue()
      value.add(new Connection(address, port))
      this.connectionSubject.next(value)
    }
  }

  removeConnection(connection: Connection) {
    this.connectionSubject.getValue().delete(connection)
  }

  public httpParams(connection: Connection): HttpParams {
    return new HttpParams().set("address", connection.address).set("port", connection.port)
  }

  public connectionName(connection: Connection) {
    return `${connection.address}:${connection.port}`
  }

}


