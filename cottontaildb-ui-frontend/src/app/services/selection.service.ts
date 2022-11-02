
import { Injectable } from '@angular/core';
import {Subject} from 'rxjs';

export interface Selection {
  entity : string;
  port : number;
}

@Injectable()
export class SelectionService {


  private selectionSubject = new Subject<Selection>();
  currentSelection = this.selectionSubject.asObservable();

  constructor() { }

  changeSelection(entity: string, port: number) {
    this.selectionSubject.next({entity, port})
  }

}