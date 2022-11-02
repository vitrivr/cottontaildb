import { Injectable } from '@angular/core';
import {BehaviorSubject} from "rxjs";
import {MatSnackBar} from "@angular/material/snack-bar";

@Injectable({
  providedIn: 'root'
})
export class ConnectionService {

  public apiURL = 'http://localhost:7070/'

  connectionSubject = new BehaviorSubject<Array<number>>([1865]);

  constructor(private snackBar: MatSnackBar) {
  }

  addConnection(port: number) {
    if (this.connectionSubject.getValue().includes(port)) {
      this.snackBar.open("Connection " + port + " is already been added.")
    } else {
      //TODO: Handle unavailable port
      this.connectionSubject.next(this.connectionSubject.getValue().concat(port))
    }
  }

  removeConnection(port: number) {
    let value = this.connectionSubject.getValue()
    let index = value.indexOf(port)
    this.connectionSubject.next(value.splice(index))
  }

}


