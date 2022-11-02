import { Injectable } from '@angular/core';
import {BehaviorSubject} from "rxjs";
import {HttpClient} from "@angular/common/http";
import {ConnectionService} from "./connection.service";

@Injectable({
  providedIn: 'root'
})
export class SystemService {


  transactionListSubject = new BehaviorSubject<any>(null);
  lockListSubject = new BehaviorSubject<any>(null);


  constructor(private httpClient:HttpClient, private connectionService: ConnectionService) { }

  listTransactions(port: number) {
    return this.httpClient.get(this.connectionService.apiURL + port + "/system/transactions")
      .subscribe(transactions => this.transactionListSubject.next(transactions))
  }

  listLocks(port: number) {
    return this.httpClient.get(this.connectionService.apiURL + port + "/system/locks")
      .subscribe(locks => this.lockListSubject.next(locks))
  }

  killTransaction(port: number, txId: any) {
    return this.httpClient.delete(this.connectionService.apiURL + port + "/system/transactions/" + txId).subscribe()
  }
}
