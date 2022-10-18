import { Injectable } from '@angular/core';
import {Observable} from "rxjs";
import {Entity} from "../interfaces/Entity";
import {HttpClient} from "@angular/common/http";
import {ColumnDef} from "../components/main/entity-view/create-entity-form/create-entity-form.component";

@Injectable({
  providedIn: 'root'
})
export class EntityService {

  private apiURL = 'http://localhost:7070'

  constructor(private httpClient:HttpClient) { }

  public getEntities() : Observable<Entity[]> {
    return this.httpClient.get<Entity[]>('${apiURL}/entity/all');
  }

  public createEntity (schemaName: string,entityName: string, columnDef : ColumnDef){
    return this.httpClient.post(this.apiURL + "/entities/" + schemaName + "." +entityName , columnDef);
  }

  dropEntity (entityName: string) {
    return this.httpClient.delete(this.apiURL + "/entities/" + entityName)
  }

  truncateEntity(entityName: string) {
    return this.httpClient.delete(this.apiURL + "/entities/" + entityName + "/truncate/")
  }

  clearEntity(entityName: string) : Observable<Object>{
    return this.httpClient.delete(this.apiURL + "/entities/" + entityName + "/clear/")
  }
}
