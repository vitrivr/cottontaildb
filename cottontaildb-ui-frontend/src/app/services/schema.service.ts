import { Injectable } from '@angular/core';
import {HttpClient} from "@angular/common/http";
import {Observable} from "rxjs";
import {Entity} from "../interfaces/Entity";

export interface Schema {
  name : string
}

@Injectable({
  providedIn: 'root'
})

export class SchemaService {

  private apiURL = 'http://localhost:7070/'

  constructor(private httpClient: HttpClient) {
  }

  public listSchemas(): Observable<Schema[]> {
    return this.httpClient
      .get<Schema[]>(this.apiURL + "schemas");
  }

  public listEntities(schema: Schema): Observable<Entity[]> {
    return this.httpClient
      .get<Entity[]>(this.apiURL + "schemas/" + schema);
  }

  public dropSchema(schema: Schema) {
    return this.httpClient.delete(this.apiURL + "schemas/" + schema);
  }

  public createSchema(schema: Schema): Observable<Schema> {
    return this.httpClient.post<Schema>(this.apiURL + "schemas/", schema);
  }

}
