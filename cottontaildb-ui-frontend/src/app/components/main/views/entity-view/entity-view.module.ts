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
import {MatInputModule} from "@angular/material/input";
import {MatProgressSpinnerModule} from "@angular/material/progress-spinner";
import {MatProgressBarModule} from "@angular/material/progress-bar";
import {EntityAboutComponent} from "./entity-about.component";
import {MatListModule} from "@angular/material/list";

@NgModule({
  declarations: [
    EntityViewComponent,
    EntityAboutComponent,
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
    MatSortModule,
    MatInputModule,
    MatProgressSpinnerModule,
    MatProgressBarModule,
    MatListModule
  ],
  providers: [EntityViewComponent],
  exports: [EntityViewComponent],
  bootstrap: []
})
export class EntityViewModule {

}
