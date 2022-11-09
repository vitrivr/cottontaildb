import { Injectable } from '@angular/core';
import {HttpClient, HttpParams} from "@angular/common/http";
import {catchError, map, throwError} from "rxjs";
import {ConnectionService} from "./connection.service";

export class QueryData{
}

export interface QueryFunction {
  name: string
  parameters: Array<any>
}

export class Select implements QueryFunction{
  name: string = "SELECT"
  parameters: Array<string> = []

  constructor(conditions: object) {
    let len = Object.values(conditions).length
    for (let i = 0; i < len ; i++){
      if (Object.values(conditions)[i]){
        this.parameters.push(Object.keys(conditions)[i])
      }}}
}

export class QueryMessage {
  functions: Array<QueryFunction> = []
  constructor(functions: Array<QueryFunction>) {
    this.functions = functions
  }
}


@Injectable({
  providedIn: 'root'
})
export class QueryService {

  constructor(private http:HttpClient,
              private connectionService: ConnectionService) {}

  query(port: number, entity: string, queryMessage: QueryMessage){

    let params = new HttpParams().set('FROM', entity)

    queryMessage.functions.forEach(queryFunction => {
      console.log(queryFunction.name)
        queryFunction.parameters.forEach(param => {
          params = params.append(queryFunction.name, param)
          console.log(params)
          console.log(param)
        })
    })

    return this.http.get(this.connectionService.apiURL + port + "/query/", {params}).pipe(
      map((queryData: QueryData) => queryData),
      catchError(err => throwError(err))
    )


  }


}
