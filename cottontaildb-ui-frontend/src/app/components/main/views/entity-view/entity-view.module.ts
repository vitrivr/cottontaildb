import {NgModule} from "@angular/core";
import {BrowserModule} from "@angular/platform-browser";
import {CommonModule} from "@angular/common";
import {MatLegacyButtonModule as MatButtonModule} from "@angular/material/legacy-button";
import {MatLegacyTableModule as MatTableModule} from "@angular/material/legacy-table";
import {MatIconModule} from "@angular/material/icon";
import {MatLegacyPaginatorModule as MatPaginatorModule} from "@angular/material/legacy-paginator";
import {MatLegacyTabsModule as MatTabsModule} from "@angular/material/legacy-tabs";
import {MatSortModule} from "@angular/material/sort";
import {EntityViewComponent} from "./entity-view.component";
import {EntityPreviewComponent} from "./entity-preview.component";
import {MatLegacyInputModule as MatInputModule} from "@angular/material/legacy-input";
import {MatLegacyProgressSpinnerModule as MatProgressSpinnerModule} from "@angular/material/legacy-progress-spinner";
import {MatLegacyProgressBarModule as MatProgressBarModule} from "@angular/material/legacy-progress-bar";
import {EntityAboutComponent} from "./entity-about.component";
import {MatLegacyListModule as MatListModule} from "@angular/material/legacy-list";

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
