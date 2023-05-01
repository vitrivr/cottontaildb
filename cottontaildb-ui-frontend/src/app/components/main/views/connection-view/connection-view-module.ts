import {NgModule} from "@angular/core";
import {ConnectionViewComponent} from "./connection-view.component";
import {BrowserModule} from "@angular/platform-browser";
import {CommonModule} from "@angular/common";
import {MatLegacyButtonModule as MatButtonModule} from "@angular/material/legacy-button";
import {MatLegacyTableModule as MatTableModule} from "@angular/material/legacy-table";
import {MatIconModule} from "@angular/material/icon";
import {MatLegacyPaginatorModule as MatPaginatorModule} from "@angular/material/legacy-paginator";
import {MatLegacyTabsModule as MatTabsModule} from "@angular/material/legacy-tabs";
import {MatSortModule} from "@angular/material/sort";
import {TransactionListComponent} from "./transaction-list-component";

@NgModule({
  declarations: [
    ConnectionViewComponent,
    TransactionListComponent
  ],
  imports: [
    BrowserModule,
    CommonModule,
    MatButtonModule,
    MatTableModule,
    MatIconModule,
    MatPaginatorModule,
    MatTabsModule,
    MatSortModule
  ],
  providers: [ConnectionViewComponent],
  exports: [ConnectionViewComponent],
  bootstrap: []
})
export class ConnectionViewModule {

}
