import {NgModule} from "@angular/core";
import {SidebarComponent} from "./sidebar.component";
import {CreateSchemaFormComponent} from "./create-schema-form/create-schema-form.component";
import {CreateEntityFormComponent} from "./create-entity-form/create-entity-form.component";
import {AddConnectionFormComponent} from "./add-connection-form/add-connection-form.component";
import {MatTreeModule} from "@angular/material/tree";
import {MatIconModule} from "@angular/material/icon";
import {FormsModule, ReactiveFormsModule} from "@angular/forms";
import {CommonModule} from "@angular/common";
import {BrowserModule} from "@angular/platform-browser";
import {MatToolbarModule} from "@angular/material/toolbar";
import {HeaderComponent} from "./header.component";
import {MatButtonModule} from "@angular/material/button";
import {MatFormFieldModule} from "@angular/material/form-field";
import {MatTableModule} from "@angular/material/table";
import {MatInputModule} from "@angular/material/input";
import {MatMenuModule} from "@angular/material/menu";
import {MatOptionModule} from "@angular/material/core";
import {MatProgressBarModule} from "@angular/material/progress-bar";
import {MatProgressSpinnerModule} from "@angular/material/progress-spinner";
import {MatDialogModule} from "@angular/material/dialog";
import {MatDividerModule} from "@angular/material/divider";
import {MatTooltipModule} from "@angular/material/tooltip";
import {MatSnackBarModule} from "@angular/material/snack-bar";

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
    MatDialogModule,
    MatButtonModule,
    MatFormFieldModule,
    MatTableModule,
    MatTreeModule,
    MatIconModule,
    MatInputModule,
    MatMenuModule,
    MatOptionModule,
    MatSnackBarModule,
    MatToolbarModule,
    MatProgressBarModule,
    MatProgressSpinnerModule,
    MatDialogModule,
    MatDividerModule,
    MatTooltipModule
  ],
  providers: [SidebarComponent, HeaderComponent],
  exports: [SidebarComponent, HeaderComponent],
  bootstrap: []
})
export class NavigationModule {

}
