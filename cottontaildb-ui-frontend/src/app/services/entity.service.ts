import { Injectable } from '@angular/core';
import {BehaviorSubject, Observable, shareReplay} from "rxjs";
import {Entity} from "../interfaces/Entity";
import {HttpClient} from "@angular/common/http";
import {ColumnDefinition} from "../interfaces/ColumnDefinition";
import {TreeNode} from "../interfaces/TreeNode";
import {IndexDefinition} from "../interfaces/IndexDefinition";

@Injectable({
  providedIn: 'root'
})
export class EntityService {

  aboutEntitySubject = new BehaviorSubject<any>(null);

  private apiURL = 'http://localhost:7070'

  constructor(private httpClient:HttpClient) { }

  public getEntities() : Observable<Entity[]> {
    return this.httpClient.get<Entity[]>('${apiURL}/entity/all');
  }

  public createEntity (schemaName: string,entityName: string, columnDef : ColumnDefinition){
    return this.httpClient.post(this.apiURL + "/entities/" + schemaName + "." +entityName , columnDef);
  }

  dropEntity (entityName: string) {
    return this.httpClient.delete(this.apiURL + "/entities/" + entityName)
  }

  truncateEntity(entityName: string) {
    return this.httpClient.delete(this.apiURL + "/entities/" + entityName + "/truncate/")
  }

  clearEntity(entityName: string) : Observable<Object>{
    return this.httpClient.delete(this.apiURL + "/entities/" + entityName + "/clear/")
  }

  aboutEntity(entityName: string) {
    this.httpClient.get(this.apiURL + "/entities/" + entityName).subscribe(value => this.aboutEntitySubject.next(value))
  }

  dropIndex(dbo: string) {
    return this.httpClient.delete(this.apiURL + "/indexes/" + dbo)
  }

  createIndex(dbo: string, indexDefinition: IndexDefinition) {
    return this.httpClient.post(this.apiURL + "/indexes/" + dbo, indexDefinition)
  }
}
