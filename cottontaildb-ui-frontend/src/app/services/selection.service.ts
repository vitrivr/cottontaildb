
import { Injectable } from '@angular/core';
import {Subject} from 'rxjs';

@Injectable()
export class SelectionService {

  private selectionSource = new Subject();
  currentSelection = this.selectionSource.asObservable();

  constructor() { }

  changeSelection(selection: string) {
    this.selectionSource.next(selection)
  }

}
