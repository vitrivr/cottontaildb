import { Injectable } from '@angular/core';
import {HttpClient, HttpParams} from "@angular/common/http";
import {catchError, map, throwError} from "rxjs";
import {ConnectionService} from "./connection.service";

export class QueryData {
}

export interface QueryFunction {
  name: string
  parameters: any
}

export class Select implements QueryFunction{
  name = "SELECT"
  parameters: [string]
  constructor(column: string) {
    this.parameters = [column]
  }
}

export class From implements QueryFunction{
  name = "FROM"
  parameters: [string]
  constructor(entity: string) {
    this.parameters = [entity]
  }
}

export class Distance implements QueryFunction{
  name = "DISTANCE"
  parameters: [string, string, string, string, string]
  constructor(entity: string, vector: string, vectorType: string, distance: string, name: string) {
    this.parameters = [entity, vector, vectorType, distance, name]
  }
}

export class Limit implements QueryFunction{
  name = "LIMIT"
  parameters: [number]
  constructor(limit: number) {
    this.parameters = [limit]
  }
}

export class Order implements QueryFunction{
  name = "ORDER"
  parameters: [string, string]
  constructor(column: string, direction: string) {
    this.parameters = [column, direction]
  }
}


@Injectable({
  providedIn: 'root'
})
export class QueryService {

  constructor(private http:HttpClient,
              private connectionService: ConnectionService) {}

  query(port: number, entity: string, queryMessage: Array<QueryFunction>, page: number, pageSize: number){

    let params = new HttpParams()
      .set("pageSize", pageSize)
      .set("page", page)

    return this.http.post(this.connectionService.apiURL + port + "/query/", queryMessage,{params}).pipe(
      map((queryData: QueryData) => queryData),
      catchError(err => throwError(err))
    )


  }


}
