import { Injectable } from '@angular/core';
import {BehaviorSubject, Observable} from "rxjs";
import {HttpClient, HttpParams} from "@angular/common/http";
import {ColumnDefinition} from "../interfaces/ColumnDefinition";
import {IndexDefinition} from "../interfaces/IndexDefinition";
import {Connection, ConnectionService} from "./connection.service";
import {MatSnackBar} from "@angular/material/snack-bar";

@Injectable({
  providedIn: 'root'
})
export class EntityService {

  aboutEntitySubject = new BehaviorSubject<any>(null);
  deleteSubject = new BehaviorSubject<any>(null);

  constructor(private httpClient:HttpClient,
              private connectionService:ConnectionService,
              private snackBar:MatSnackBar) {
  }

  public createEntity (connection: Connection, schemaName: string,entityName: string, columnDef : ColumnDefinition){
    let params = this.connectionService.httpParams(connection)
    return this.httpClient.post(this.connectionService.apiURL + "entities/" + schemaName + "." +entityName , columnDef, {params});
  }

  dropEntity (connection: Connection, entityName: string) {
    let params = this.connectionService.httpParams(connection)
    return this.httpClient.delete(this.connectionService.apiURL + "entities/" + entityName, {params})
  }

  truncateEntity(connection: Connection, entityName: string) {
    let params = this.connectionService.httpParams(connection)
    return this.httpClient.delete(this.connectionService.apiURL + "entities/" + entityName + "/truncate/", {params})
  }

  clearEntity(connection: Connection, entityName: string) : Observable<Object>{
    let params = this.connectionService.httpParams(connection)
    return this.httpClient.delete(this.connectionService.apiURL + "entities/" + entityName + "/clear/", {params})
  }

  aboutEntity(connection: Connection, entityName: string) {
    let params = this.connectionService.httpParams(connection)
    this.httpClient.get(this.connectionService.apiURL.concat("entities/", entityName), {params}).subscribe(
      {next: value => {
          /** Only call next if value has actually changed **/
          if(JSON.stringify(this.aboutEntitySubject.value) !== JSON.stringify(value)) {
            this.aboutEntitySubject.next(value)
          }},
        error: err => {
          this.aboutEntitySubject.next(null)
          this.snackBar.open(`Error ${err.status}: ${err.statusText}`, "ok")
        }
      }
    )
  }

  dropIndex(connection: Connection, dbo: string) {
    let params = this.connectionService.httpParams(connection)
    return this.httpClient.delete(this.connectionService.apiURL + "indexes/" + dbo, {params})
  }

  createIndex(connection: Connection, dbo: string, indexDefinition: IndexDefinition) {
    let params = this.connectionService.httpParams(connection)
    return this.httpClient.post(this.connectionService.apiURL + "indexes/" + dbo, indexDefinition, {params})
  }

  dumpEntity(connection: Connection, name: string) {
    let params = this.connectionService.httpParams(connection)
    return this.httpClient.get(this.connectionService.apiURL + "entities/" + name + "/data", {params})
  }

  deleteRow(connection: Connection, name: string, column: string, operator: string, value: string, type: string) {
    let params = new HttpParams
    params = params.set("column", column).set("operator", operator).set("value", value).set("entity", name).set("type", type)
    this.httpClient.delete(this.connectionService.apiURL + "entities/" + name + "/data/", {params}).subscribe(
      {next: value => {
        if(JSON.stringify(this.deleteSubject.value) !== JSON.stringify(value)) {
          this.deleteSubject.next(value)
      }}, error: err => {
          this.deleteSubject.next(null)
          this.snackBar.open(`Error ${err.status}: ${err.statusText}`, "ok")
        }
    })
  }

  insertRow(connection: Connection, name: string){
    let params = this.connectionService.httpParams(connection)
    return this.httpClient.post(this.connectionService.apiURL + "entities/" + name + "/data/", null, {params})
  }

  updateRow(connection: Connection, name: string){
    let params = this.connectionService.httpParams(connection)
    return this.httpClient.patch(this.connectionService.apiURL + "entities/" + name + "/data/", null, {params})
  }
}
