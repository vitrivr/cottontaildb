import { Component, OnInit } from '@angular/core';
import {SystemService} from "../../../../services/system.service";
import {ConnectionService} from "../../../../services/connection.service";

@Component({
  selector: 'app-system-view',
  templateUrl: './system-view.component.html',
  styleUrls: ['./system-view.component.css']
})
export class SystemViewComponent implements OnInit {
  txData: any;
  locksData: any;
  connections: any;

  constructor(private systemService: SystemService,
              private connectionService: ConnectionService) { }

  ngOnInit(): void {
    //TODO: not quite right yet, add this functionality in new child component
    this.connectionService.connectionSubject.subscribe(connections => this.connections = connections)
    this.systemService.listTransactions(this.connections)
    this.systemService.listLocks(this.connections)
    this.systemService.transactionListSubject.subscribe(tx => this.txData = tx)
    this.systemService.lockListSubject.subscribe(locks => this.locksData = locks)

  }

  onKillTx(txId: any) {
    this.systemService.killTransaction(this.connections, txId)
  }

}
