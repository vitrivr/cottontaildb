import { Component, OnInit } from '@angular/core';
import {SystemService} from "../../../services/system.service";

@Component({
  selector: 'app-system-view',
  templateUrl: './system-view.component.html',
  styleUrls: ['./system-view.component.css']
})
export class SystemViewComponent implements OnInit {
  txData: any;
  locksData: any;

  constructor(private systemService: SystemService) { }

  ngOnInit(): void {

    this.systemService.listTransactions()
    this.systemService.listLocks()
    this.systemService.transactionListSubject.subscribe(tx => this.txData = tx)
    this.systemService.lockListSubject.subscribe(locks => this.locksData = locks)

  }

  onKillTx(txId: any) {
  }

}
