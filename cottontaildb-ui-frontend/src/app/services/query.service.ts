import { Injectable } from '@angular/core';
import {HttpClient, HttpParams} from "@angular/common/http";
import {catchError, map, throwError} from "rxjs";
import {ConnectionService} from "./connection.service";

export class QueryData {
}

export interface QueryFunction {
  name: string
}

export class Select implements QueryFunction{
  name = "SELECT";
  parameters: [string]
  constructor(column: string) {
    this.parameters = [column]
  }
}

export class From implements QueryFunction{
  name = "FROM";
  parameters: [string]
  constructor(entity: string) {
    this.parameters = [entity]
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
