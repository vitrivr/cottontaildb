import { NgModule } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';
import { AppComponent } from './app.component';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { MainComponent } from './components/main/main.component';
import { MatTabsModule} from "@angular/material/tabs";
import { SidebarComponent} from "./components/sidebar/sidebar.component";
import { HeaderComponent } from './components/main/header/header.component';
import { OverviewComponent } from './components/sidebar/overview/overview.component';
import { TreeComponent } from './components/sidebar/overview/tree/tree.component';
import { MatTreeModule } from '@angular/material/tree';
import { MatIconModule } from '@angular/material/icon';
import {DragDropModule} from "@angular/cdk/drag-drop";
import { DmlViewComponent } from './components/main/views/dml-view/dml-view.component';
import { QueryViewComponent } from './components/main/views/query-view/query-view.component';
import { SystemViewComponent } from './components/main/views/system-view/system-view.component';
import { ButtonComponent } from './components/utilities/button/button.component';
import { DropZoneComponent } from './components/utilities/drop-zone/drop-zone.component';
import { HttpClientModule } from '@angular/common/http';
import { AppRoutingModule } from './app-routing.module';
import { SchemaViewComponent } from './components/main/views/schema-view/schema-view.component';
import { MatTooltipModule } from "@angular/material/tooltip";
import { MatButtonModule } from "@angular/material/button";
import { MatInputModule } from "@angular/material/input";
import { ReactiveFormsModule, FormsModule}  from "@angular/forms";
import { CreateSchemaFormComponent } from './components/sidebar/overview/create-schema-form/create-schema-form.component';
import { CreateEntityFormComponent } from './components/sidebar/overview/create-entity-form/create-entity-form.component';
import {MatStepperModule} from "@angular/material/stepper";
import {MatSelectModule} from "@angular/material/select";
import {MatCheckboxModule} from "@angular/material/checkbox";
import {MatTableModule} from "@angular/material/table";
import { MatPaginatorModule } from '@angular/material/paginator';
import { MatSortModule } from '@angular/material/sort';
import {MatMenuModule} from "@angular/material/menu";
import {MatSnackBarModule} from "@angular/material/snack-bar";
import {MAT_DIALOG_DATA, MatDialogModule, MatDialogRef} from "@angular/material/dialog";
import {DdlViewComponent} from "./components/main/views/ddl-view/ddl-view.component";
import {SelectionService} from "./services/selection.service";
import {MatButtonToggleModule} from "@angular/material/button-toggle";
import { CreateIndexFormComponent } from './components/main/views/ddl-view/create-index-form/create-index-form.component';
import { AddConnectionFormComponent } from './components/sidebar/overview/add-connection-form/add-connection-form.component';
import {MatProgressSpinnerModule} from "@angular/material/progress-spinner";
import {MatProgressBarModule} from "@angular/material/progress-bar";
import { GroupControlComponent } from './components/main/views/query-view/group-control/group-control.component';
import { ConditionFormComponent } from './components/main/views/query-view/condition-form/condition-form.component';
import { ActionButtonsComponent } from './components/main/views/query-view/action-buttons/action-buttons.component';

@NgModule({
  declarations: [
    AppComponent,
    SidebarComponent,
    MainComponent,
    HeaderComponent,
    OverviewComponent,
    TreeComponent,
    DmlViewComponent,
    QueryViewComponent,
    SystemViewComponent,
    ButtonComponent,
    DropZoneComponent,
    SchemaViewComponent,
    DdlViewComponent,
    CreateSchemaFormComponent,
    CreateEntityFormComponent,
    CreateIndexFormComponent,
    AddConnectionFormComponent,
    GroupControlComponent,
    ConditionFormComponent,
    ActionButtonsComponent
  ],
  imports: [
    BrowserModule,
    BrowserAnimationsModule,
    MatTabsModule,
    MatTreeModule,
    MatIconModule,
    DragDropModule,
    HttpClientModule,
    AppRoutingModule,
    MatTooltipModule,
    MatButtonModule,
    MatInputModule,
    ReactiveFormsModule,
    FormsModule,
    MatStepperModule,
    MatSelectModule,
    MatCheckboxModule,
    MatTableModule,
    MatPaginatorModule,
    MatSortModule,
    MatMenuModule,
    MatSnackBarModule,
    MatDialogModule,
    MatButtonToggleModule,
    MatProgressSpinnerModule,
    MatProgressBarModule
  ],
  providers: [
    CreateEntityFormComponent,
    SelectionService,
    { provide: MAT_DIALOG_DATA, useValue: {} },
    { provide: MatDialogRef, useValue: {} }
  ],
  exports: [
  ],
  bootstrap: [AppComponent]
})
export class AppModule { }