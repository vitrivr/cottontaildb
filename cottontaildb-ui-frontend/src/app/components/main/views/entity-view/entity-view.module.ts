import {NgModule} from "@angular/core";
import {BrowserModule} from "@angular/platform-browser";
import {CommonModule} from "@angular/common";
import {MatButtonModule} from "@angular/material/button";
import {MatTableModule} from "@angular/material/table";
import {MatIconModule} from "@angular/material/icon";
import {MatPaginatorModule} from "@angular/material/paginator";
import {MatTabsModule} from "@angular/material/tabs";
import {MatSortModule} from "@angular/material/sort";
import {EntityViewComponent} from "./entity-view.component";
import {EntityPreviewComponent} from "./entity-preview.component";

@NgModule({
  declarations: [
    EntityViewComponent,
    EntityPreviewComponent
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
  providers: [EntityViewComponent],
  exports: [EntityViewComponent],
  bootstrap: []
})
export class EntityViewModule {

}
