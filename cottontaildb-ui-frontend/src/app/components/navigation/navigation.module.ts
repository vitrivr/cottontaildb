import {NgModule} from "@angular/core";
import {SidebarComponent} from "./sidebar.component";
import {CreateSchemaFormComponent} from "./create-schema-form/create-schema-form.component";
import {CreateEntityFormComponent} from "./create-entity-form/create-entity-form.component";
import {AddConnectionFormComponent} from "./add-connection-form/add-connection-form.component";
import {MatTreeModule} from "@angular/material/tree";
import {MatIconModule} from "@angular/material/icon";
import {MatMenuModule} from "@angular/material/menu";
import {MatFormFieldModule} from "@angular/material/form-field";
import {FormsModule, ReactiveFormsModule} from "@angular/forms";
import {MatTableModule} from "@angular/material/table";
import {MatOptionModule} from "@angular/material/core";
import {MatButtonModule} from "@angular/material/button";
import {MatInputModule} from "@angular/material/input";
import {CommonModule} from "@angular/common";
import {BrowserModule} from "@angular/platform-browser";
import {MatToolbarModule} from "@angular/material/toolbar";
import {MatProgressBarModule} from "@angular/material/progress-bar";
import {MatProgressSpinnerModule} from "@angular/material/progress-spinner";
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
