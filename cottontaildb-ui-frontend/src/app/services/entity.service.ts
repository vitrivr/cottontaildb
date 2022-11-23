import { Injectable } from '@angular/core';
import {BehaviorSubject, Observable} from "rxjs";
import {HttpClient, HttpParams} from "@angular/common/http";
import {ColumnDefinition} from "../interfaces/ColumnDefinition";
import {IndexDefinition} from "../interfaces/IndexDefinition";
import {ConnectionService} from "./connection.service";
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
    this.httpClient.get(this.connectionService.apiURL.concat(port.toString(),"/entities/",entityName)).subscribe(
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

  dropIndex(port: number, dbo: string) {
    return this.httpClient.delete(this.connectionService.apiURL + port.toString() + "/indexes/" + dbo)
  }

  createIndex(port: number, dbo: string, indexDefinition: IndexDefinition) {
    return this.httpClient.post(this.connectionService.apiURL + port + "/indexes/" + dbo, indexDefinition)
  }

  dumpEntity(port: number, name: string) {
    return this.httpClient.get(this.connectionService.apiURL + port + "/entities/" + name + "/data")
  }

  deleteRow(port: number, name: string, column: string, operator: string, value: string, type: string) {
    let params = new HttpParams
    params = params.set("column", column).set("operator", operator).set("value", value).set("entity", name).set("type", type)
    this.httpClient.delete(this.connectionService.apiURL + port + "/entities/" + name + "/data/", {params}).subscribe(
      {next: value => {
        if(JSON.stringify(this.deleteSubject.value) !== JSON.stringify(value)) {
          this.deleteSubject.next(value)
      }}, error: err => {
          this.deleteSubject.next(null)
          this.snackBar.open(`Error ${err.status}: ${err.statusText}`, "ok")
        }
    })
  }

  insertRow(port: number, name: string){
    return this.httpClient.post(this.connectionService.apiURL + port + "/entities/" + name + "/data/", null)
  }

  updateRow(port: number, name: string){
    return this.httpClient.patch(this.connectionService.apiURL + port + "/entities/" + name + "/data/", null)
  }
}
