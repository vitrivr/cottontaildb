import { Injectable } from '@angular/core';
import {BehaviorSubject, Observable} from "rxjs";
import {HttpClient} from "@angular/common/http";
import {ColumnDefinition} from "../interfaces/ColumnDefinition";
import {IndexDefinition} from "../interfaces/IndexDefinition";
import {ConnectionService} from "./connection.service";

@Injectable({
  providedIn: 'root'
})
export class EntityService {

  aboutEntitySubject = new BehaviorSubject<any>(null);

  constructor(private httpClient:HttpClient,
              private connectionService:ConnectionService) {
    console.log("Entity Service")
  }


  public createEntity (port: number, schemaName: string,entityName: string, columnDef : ColumnDefinition){
    return this.httpClient.post(this.connectionService.apiURL + port + "/entities/" + schemaName + "." +entityName , columnDef);
  }

  dropEntity (port: number, entityName: string) {
    return this.httpClient.delete(this.connectionService.apiURL + port + "/entities/" + entityName)
  }

  truncateEntity(port: number, entityName: string) {
    return this.httpClient.delete(this.connectionService.apiURL + port + "/entities/" + entityName + "/truncate/")
  }

  clearEntity(port: number, entityName: string) : Observable<Object>{
    return this.httpClient.delete(this.connectionService.apiURL + port + "/entities/" + entityName + "/clear/")
  }

  aboutEntity(port: number, entityName: string) {
    this.httpClient.get(this.connectionService.apiURL.concat(port.toString(),"/entities/",entityName)).subscribe(value => this.aboutEntitySubject.next(value))
  }

  dropIndex(port: number, dbo: string) {
    return this.httpClient.delete(this.connectionService.apiURL + port.toString() + "/indexes/" + dbo)
  }

  createIndex(port: number, dbo: string, indexDefinition: IndexDefinition) {
    return this.httpClient.post(this.connectionService.apiURL + port + "/indexes/" + dbo, indexDefinition)
  }

  dumpEntity(port: number, name: string) {
    return this.httpClient.get(this.connectionService.apiURL + port + "/entities/" + name + "/data")
  }
}
