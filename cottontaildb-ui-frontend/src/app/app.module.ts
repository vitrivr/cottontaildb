import { NgModule } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';
import { AppComponent } from './app.component';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { MainComponent } from './components/main/main.component';
import { MatTabsModule} from "@angular/material/tabs";
import { HeaderComponent} from "./components/header/header.component";
import { MatIconModule } from '@angular/material/icon';
import {DragDropModule} from "@angular/cdk/drag-drop";
import { DmlViewComponent } from './components/main/views/dml-view/dml-view.component';
import { QueryViewComponent } from './components/main/views/query-view/query-view.component';
import { SystemViewComponent } from './components/main/views/system-view/system-view.component';
import { HttpClientModule } from '@angular/common/http';
import { AppRoutingModule } from './app-routing.module';
import { MatTooltipModule } from "@angular/material/tooltip";
import { MatButtonModule } from "@angular/material/button";
import { MatInputModule } from "@angular/material/input";
import { ReactiveFormsModule, FormsModule}  from "@angular/forms";
import { CreateEntityFormComponent } from './components/sidebar/create-entity-form/create-entity-form.component';
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
import {MatProgressSpinnerModule} from "@angular/material/progress-spinner";
import {MatProgressBarModule} from "@angular/material/progress-bar";
import { SelectFormComponent } from './components/main/views/query-view/select-form/select-form.component';
import { WhereFormComponent } from './components/main/views/query-view/where-form/where-form.component';
import { OrderFormComponent } from './components/main/views/query-view/order-form/order-form.component';
import { DistanceFormComponent } from './components/main/views/query-view/distance-form/distance-form.component';
import { LimitFormComponent } from './components/main/views/query-view/limit-form/limit-form.component';
import {MatAutocompleteModule} from "@angular/material/autocomplete";
import {MatCardModule} from "@angular/material/card";
import {MatDividerModule} from "@angular/material/divider";
import { CountFormComponent } from './components/main/views/query-view/count-form/count-form.component';
import { UseIndexFormComponent } from './components/main/views/query-view/use-index-form/use-index-form.component';
import { UseIndexTypeFormComponent } from './components/main/views/query-view/use-index-type-form/use-index-type-form.component';
import { DisallowParallelismFormComponent } from './components/main/views/query-view/disallow-parallelism-form/disallow-parallelism-form.component';
import { LimitParallelismFormComponent } from './components/main/views/query-view/limit-parallelism-form/limit-parallelism-form.component';
import {MatChipsModule} from "@angular/material/chips";
import { DeleteFormComponent } from './components/main/views/dml-view/delete-form/delete-form.component';
import { InsertFormComponent } from './components/main/views/dml-view/insert-form/insert-form.component';
import { UpdateFormComponent } from './components/main/views/dml-view/update-form/update-form.component';
import { VectorDetailsComponent } from './components/main/views/query-view/vector-details/vector-details.component';
import { WelcomeViewComponent } from './components/main/views/welcome-view/welcome-view.component';
import {ApiModule, Configuration} from "../../openapi";
import {SidebarModule} from "./components/sidebar/sidebar.module";



/**
 * Provides the {@link AppConfig} reference.
 *
 * @param appConfig Reference (provided by DI).
 */
export function initializeApiConfig() {
  return new Configuration({ basePath: "http://localhost:7070", withCredentials: true });
}

@NgModule({
  declarations: [
    AppComponent,
    MainComponent,
    HeaderComponent,
    DmlViewComponent,
    QueryViewComponent,
    SystemViewComponent,
    DdlViewComponent,
    CreateIndexFormComponent,
    SelectFormComponent,
    WhereFormComponent,
    OrderFormComponent,
    DistanceFormComponent,
    LimitFormComponent,
    CountFormComponent,
    UseIndexFormComponent,
    UseIndexTypeFormComponent,
    DisallowParallelismFormComponent,
    LimitParallelismFormComponent,
    DeleteFormComponent,
    InsertFormComponent,
    UpdateFormComponent,
    VectorDetailsComponent,
    WelcomeViewComponent
  ],
    imports: [
        {ngModule: ApiModule, providers: [{ provide: Configuration, useFactory: initializeApiConfig }] },
        BrowserModule,
        BrowserAnimationsModule,
        MatTabsModule,
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
        MatProgressBarModule,
        MatAutocompleteModule,
        MatCardModule,
        MatDividerModule,
        MatChipsModule,
        SidebarModule
    ],
  providers: [
    CreateEntityFormComponent,
    SelectionService,
    { provide: MAT_DIALOG_DATA, useValue: {} },
    { provide: MatDialogRef, useValue: {} },
  ],
  exports: [
  ],
  bootstrap: [AppComponent]
})
export class AppModule { }
