import {Injectable} from '@angular/core';
import {TreeNode} from "../interfaces/TreeNode";
import {BehaviorSubject, Observable, shareReplay} from "rxjs";
import {HttpClient} from "@angular/common/http";



@Injectable({
  providedIn: 'root'
})

export class TreeDataService {



  private apiURL = 'http://localhost:7070/'

  public readonly treeDataObs$ = new BehaviorSubject<Map<number, TreeNode[]>>(new Map);

  constructor(private httpClient:HttpClient){
  }

  fetchTreeData(port : number) {
    this.httpClient.get<TreeNode[]>(this.apiURL + port + "/list").pipe(shareReplay(1))
      .subscribe({
        next: (value => this.treeDataObs$.next(this.treeDataObs$.getValue().set(port, value))),
        error: (err => console.log(err))
      }).add()
  }

  getTreeData() : Observable<Map<number, TreeNode[]>> {
    return this.treeDataObs$.asObservable()
  }




}
