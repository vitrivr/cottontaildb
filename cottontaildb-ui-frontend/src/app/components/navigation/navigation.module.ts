import {NgModule} from "@angular/core";
import {SidebarComponent} from "./sidebar.component";
import {CreateSchemaFormComponent} from "./create-schema-form/create-schema-form.component";
import {CreateEntityFormComponent} from "./create-entity-form/create-entity-form.component";
import {AddConnectionFormComponent} from "./add-connection-form/add-connection-form.component";
import {MatTreeModule} from "@angular/material/tree";
import {MatIconModule} from "@angular/material/icon";
import {MatLegacyMenuModule as MatMenuModule} from "@angular/material/legacy-menu";
import {MatLegacyFormFieldModule as MatFormFieldModule} from "@angular/material/legacy-form-field";
import {FormsModule, ReactiveFormsModule} from "@angular/forms";
import {MatLegacyTableModule as MatTableModule} from "@angular/material/legacy-table";
import {MatLegacyOptionModule as MatOptionModule} from "@angular/material/legacy-core";
import {MatLegacyButtonModule as MatButtonModule} from "@angular/material/legacy-button";
import {MatLegacyInputModule as MatInputModule} from "@angular/material/legacy-input";
import {CommonModule} from "@angular/common";
import {BrowserModule} from "@angular/platform-browser";
import {MatToolbarModule} from "@angular/material/toolbar";
import {MatLegacyProgressBarModule as MatProgressBarModule} from "@angular/material/legacy-progress-bar";
import {MatLegacyProgressSpinnerModule as MatProgressSpinnerModule} from "@angular/material/legacy-progress-spinner";
import {HeaderComponent} from "./header.component";

@NgModule({
  declarations: [
    SidebarComponent,
    HeaderComponent,
    CreateSchemaFormComponent,
    CreateEntityFormComponent,
    AddConnectionFormComponent
  ],
  imports: [
    CommonModule,
    BrowserModule,
    ReactiveFormsModule,
    FormsModule,
    MatButtonModule,
    MatFormFieldModule,
    MatTableModule,
    MatTreeModule,
    MatIconModule,
    MatInputModule,
    MatMenuModule,
    MatOptionModule,
    MatToolbarModule,
    MatProgressBarModule,
    MatProgressSpinnerModule
  ],
  providers: [SidebarComponent, HeaderComponent],
  exports: [SidebarComponent, HeaderComponent],
  bootstrap: []
})
export class NavigationModule {

}
