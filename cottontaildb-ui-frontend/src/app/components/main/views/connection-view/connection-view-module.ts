import {NgModule} from "@angular/core";
import {ConnectionViewComponent} from "./connection-view.component";
import {BrowserModule} from "@angular/platform-browser";
import {CommonModule} from "@angular/common";
import {MatButtonModule} from "@angular/material/button";
import {MatTableModule} from "@angular/material/table";
import {MatIconModule} from "@angular/material/icon";
import {MatPaginatorModule} from "@angular/material/paginator";
import {MatTabsModule} from "@angular/material/tabs";
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
