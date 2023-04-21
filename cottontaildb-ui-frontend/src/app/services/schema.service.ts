import { Injectable } from '@angular/core';
import {HttpClient} from "@angular/common/http";
import {Observable} from "rxjs";
import {Entity} from "../interfaces/Entity";
import {Connection, ConnectionService} from "./connection.service";

export interface Schema {
  name : string
}

@Injectable({
  providedIn: 'root'
})

export class SchemaService {

  private apiURL = 'http://localhost:7070/'

  constructor(private httpClient: HttpClient, private connectionService: ConnectionService) {
  }

  public listSchemas(connection: Connection): Observable<Schema[]> {
    let params = this.connectionService.httpParams(connection)
    return this.httpClient.get<Schema[]>(this.apiURL + "schemas", {params});
  }

  public listEntities(connection: Connection, schema: Schema): Observable<Entity[]> {
    let params = this.connectionService.httpParams(connection)
    return this.httpClient.get<Entity[]>(this.apiURL + "schemas/" + schema,{params});
  }

  public dropSchema(connection: Connection, schema: string) {
    let params = this.connectionService.httpParams(connection)
    return this.httpClient.delete(this.apiURL + "schemas/" + schema, {params});
  }

  public createSchema(connection: Connection, schema: string): Observable<Schema> {
    let params = this.connectionService.httpParams(connection)
    console.log(params)
    return this.httpClient.post<Schema>(this.apiURL + "schemas/" + schema, null, {params});
  }

}
