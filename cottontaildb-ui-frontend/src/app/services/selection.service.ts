
import { Injectable } from '@angular/core';
import {BehaviorSubject} from 'rxjs';
import {Connection} from "./connection.service";

export interface Selection {
  connection? : Connection;
  entity? : string;
}

@Injectable()
export class SelectionService {


  private selectionSubject = new BehaviorSubject<Selection>({connection: undefined, entity: undefined});
  currentSelection = this.selectionSubject.asObservable();

  constructor() { }

  changeSelection(connection: Connection, entity: string) {
    this.selectionSubject.next({connection, entity})
  }

}
