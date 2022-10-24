import { Injectable } from '@angular/core';
import {BehaviorSubject} from "rxjs";
import {HttpClient} from "@angular/common/http";

@Injectable({
  providedIn: 'root'
})
export class SystemService {

  private apiURL = 'http://localhost:7070'

  transactionListSubject = new BehaviorSubject<any>(null);
  lockListSubject = new BehaviorSubject<any>(null);


  constructor(private httpClient:HttpClient) { }

  listTransactions() {
    return this.httpClient.get(this.apiURL + "/system/transactions")
      .subscribe(transactions => this.transactionListSubject.next(transactions))
  }

  listLocks() {
    return this.httpClient.get(this.apiURL + "/system/locks")
      .subscribe(locks => this.lockListSubject.next(locks))
  }
}
