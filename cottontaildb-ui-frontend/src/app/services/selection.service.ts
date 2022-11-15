
import { Injectable } from '@angular/core';
import {BehaviorSubject} from 'rxjs';

export interface Selection {
  entity? : string;
  port? : number;
}

@Injectable()
export class SelectionService {


  private selectionSubject = new BehaviorSubject<Selection>({entity: undefined, port: undefined});
  currentSelection = this.selectionSubject.asObservable();

  constructor() { }

  changeSelection(entity: string, port: number) {
    this.selectionSubject.next({entity, port})
  }

}
