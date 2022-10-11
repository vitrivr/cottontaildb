import { Injectable } from '@angular/core';
import {TreeNode} from "../interfaces/TreeNode";
import {Observable, of} from "rxjs";
import {HttpClient} from "@angular/common/http";

@Injectable({
  providedIn: 'root'
})
export class TreeDataService {

  private apiURL = 'http://localhost:7070/list'

  constructor(private httpClient:HttpClient) {
  }

  getTreeData() : Observable<TreeNode[]> {
    return this.httpClient.get<TreeNode[]>(this.apiURL)
    console.log("RETURNED")
  }



}
