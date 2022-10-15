import { Injectable } from '@angular/core';
import {TreeNode} from "../interfaces/TreeNode";
import {BehaviorSubject, Observable, shareReplay} from "rxjs";
import {HttpClient} from "@angular/common/http";

@Injectable({
  providedIn: 'root'
})
export class TreeDataService {

  private apiURL = 'http://localhost:7070/'
  public readonly treeDataObs$: BehaviorSubject<TreeNode[]> = new BehaviorSubject<TreeNode[]>([]);
  constructor(private httpClient:HttpClient) {
  }

  fetchTreeData() {
    this.httpClient.get<TreeNode[]>(this.apiURL + "list").pipe(shareReplay(1))
      .subscribe({
        next: (value => this.treeDataObs$.next(value)),
        error: (err => console.log(err))
      }).add(() => console.log("will be executed whatsoever"))
  }

  getTreeData() : Observable<TreeNode[]> {
    return this.treeDataObs$.asObservable()
  }



}
