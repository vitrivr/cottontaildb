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
    for(let connection of this.connections){
      this.systemService.listTransactions(connection)
      this.systemService.listLocks(connection)
      this.systemService.transactionListSubject.subscribe(tx => this.txData = tx)
      this.systemService.lockListSubject.subscribe(locks => this.locksData = locks)
    }
  }

  onKillTx(txId: any) {
    this.systemService.killTransaction(this.connections, txId)
  }

  connectionName = this.connectionService.connectionName


}
