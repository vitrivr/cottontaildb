import { Injectable } from '@angular/core';
import {Observable} from "rxjs";
import {Entity} from "../interfaces/Entity";
import {HttpClient} from "@angular/common/http";

@Injectable({
  providedIn: 'root'
})
export class EntityService {

  private apiURL = 'https://63418c9f16ffb7e275d3b122.mockapi.io/TreeNodeArray'

  constructor(private httpClient:HttpClient) { }

  public getEntities() : Observable<Entity[]> {
    return this.httpClient.get<Entity[]>('${this.apiURL}/entity/all');
  }


}
