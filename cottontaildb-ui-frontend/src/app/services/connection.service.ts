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

  connectionSubject = new BehaviorSubject<Array<Connection>>([]);

  constructor(private snackBar: MatSnackBar) {
  }

  addConnection(address: string, port: number) {
    this.connectionSubject.getValue().forEach(connection => {
      if (connection.port == port) {
        this.snackBar.open("already exists", "ok", {duration: 2000})
        return
      }
    })
    let value = this.connectionSubject.getValue()
    value.push(new Connection(address, port))
    this.connectionSubject.next(value)
  }

  removeConnection(connection: Connection) {
    let value = this.connectionSubject.getValue()
    let index = value.indexOf(connection)
    this.connectionSubject.next(value.splice(index))
  }

  public httpParams(connection: Connection): HttpParams {
    return new HttpParams().set("address", connection.address).set("port", connection.port)
  }

}


