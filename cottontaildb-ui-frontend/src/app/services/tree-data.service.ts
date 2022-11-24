import {Injectable} from '@angular/core';
import {TreeNode} from "../interfaces/TreeNode";
import {BehaviorSubject, Observable, shareReplay} from "rxjs";
import {HttpClient} from "@angular/common/http";
import {Connection, ConnectionService} from "./connection.service";



@Injectable({
  providedIn: 'root'
})

export class TreeDataService {



  private apiURL = 'http://localhost:7070/'

  public readonly treeDataObs$ = new BehaviorSubject<Map<Connection, TreeNode[]>>(new Map);

  constructor(private httpClient:HttpClient, private connectionService: ConnectionService){
  }

  fetchTreeData(connection : Connection) {
    let params = this.connectionService.httpParams(connection)
    this.httpClient.get<TreeNode[]>(this.apiURL + "list", {params}).pipe(shareReplay(1))
      .subscribe({
        next: (value => this.treeDataObs$.next(this.treeDataObs$.getValue().set(connection, value))),
        error: (err => console.log(err))
      }).add()
  }

  getTreeData(): Observable<Map<Connection, TreeNode[]>> {
    return this.treeDataObs$.asObservable()
  }




}
