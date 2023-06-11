import { Injectable } from '@angular/core';
import {BehaviorSubject, mergeMap} from "rxjs";
import {HttpParams} from "@angular/common/http";
import {Connection, SessionService} from "../../../openapi";

@Injectable({
  providedIn: 'root'
})
export class ConnectionService {

  public apiURL = 'http://localhost:7070/'

  connectionSubject = new BehaviorSubject<Array<Connection>>(new Array<Connection>);

  constructor(private service: SessionService) {
  }

  /**
   * Tries to establish a new connection to a Cottontail DB instance at given address with given port.
   *
   * @param connection
   */
  public connect(connection: Connection) {
    this.service.postApiSessionConnect(connection).subscribe(c => {
      this.connectionSubject.next(c)
    })
  }

  /**
   * Tries to disconnect an existing connection to a Cottontail DB instance.
   *
   * @param connection
   */
  public disconnect(connection: Connection) {
    this.service.postApiSessionDisconnect(connection).subscribe(c => {
      this.connectionSubject.next(c)
    })
  }

  /**
   * Refreshes the list of connections.
   */
  public refresh() {
    this.service.getApiSessionConnections().subscribe(c => {
      this.connectionSubject.next(c)
    })
  }

  public httpParams(connection: Connection): HttpParams {
    return new HttpParams().set("address", connection.host).set("port", connection.port)
  }

  public connectionName(connection: Connection) {
    return `${connection.host}:${connection.port}`
  }

}


