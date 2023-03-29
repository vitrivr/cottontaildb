import { Injectable } from '@angular/core';
import {BehaviorSubject} from "rxjs";
import {HttpClient} from "@angular/common/http";
import {Connection, ConnectionService} from "./connection.service";

@Injectable({
  providedIn: 'root'
})
export class SystemService {


  transactionListSubject = new BehaviorSubject<any>(null);
  lockListSubject = new BehaviorSubject<any>(null);

  constructor(private httpClient:HttpClient, private connectionService: ConnectionService) { }

  listTransactions(connection: Connection) {
    let params = this.connectionService.httpParams(connection)
    return this.httpClient.get(this.connectionService.apiURL  + "system/transactions", {params})
      .subscribe(transactions => this.transactionListSubject.next(transactions))
  }

  listLocks(connection: Connection) {
    let params = this.connectionService.httpParams(connection)
    return this.httpClient.get(this.connectionService.apiURL + "system/locks", {params})
      .subscribe(locks => this.lockListSubject.next(locks))
  }

  killTransaction(connection: Connection, txId: any) {
    let params = this.connectionService.httpParams(connection)
    return this.httpClient.delete(this.connectionService.apiURL + "system/transactions/" + txId, {params}).subscribe()
  }

}
