import { Injectable } from '@angular/core';
import {HttpClient} from "@angular/common/http";
import {Observable} from "rxjs";
import {Schema} from "../interfaces/Schema";

@Injectable({
  providedIn: 'root'
})

export class SchemaService {

  private apiURL = 'https://63418c9f16ffb7e275d3b122.mockapi.io/'

  constructor(private httpClient: HttpClient) { }

  public getSchemas() : Observable<Schema[]> {
    return this.httpClient.get<Schema[]>('${this.apiURL}/schema/all');
  }

}
