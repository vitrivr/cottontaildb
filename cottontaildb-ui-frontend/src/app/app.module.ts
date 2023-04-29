import {NgModule} from '@angular/core';
import {BrowserModule} from '@angular/platform-browser';
import {AppComponent} from './app.component';
import {BrowserAnimationsModule} from '@angular/platform-browser/animations';
import {MatTabsModule} from "@angular/material/tabs";
import {MatIconModule} from '@angular/material/icon';
import {DragDropModule} from "@angular/cdk/drag-drop";
import {HttpClientModule} from '@angular/common/http';
import {AppRoutingModule} from './app-routing.module';
import {MatTooltipModule} from "@angular/material/tooltip";
import {MatButtonModule} from "@angular/material/button";
import {MatInputModule} from "@angular/material/input";
import {FormsModule, ReactiveFormsModule} from "@angular/forms";
import {CreateEntityFormComponent} from './components/navigation/create-entity-form/create-entity-form.component';
import {MatStepperModule} from "@angular/material/stepper";
import {MatSelectModule} from "@angular/material/select";
import {MatCheckboxModule} from "@angular/material/checkbox";
import {MatTableModule} from "@angular/material/table";
import {MatPaginatorModule} from '@angular/material/paginator';
import {MatSortModule} from '@angular/material/sort';
import {MatMenuModule} from "@angular/material/menu";
import {MatSnackBarModule} from "@angular/material/snack-bar";
import {MAT_DIALOG_DATA, MatDialogModule, MatDialogRef} from "@angular/material/dialog";
import {SelectionService} from "./services/selection.service";
import {MatButtonToggleModule} from "@angular/material/button-toggle";
import {MatProgressSpinnerModule} from "@angular/material/progress-spinner";
import {MatProgressBarModule} from "@angular/material/progress-bar";
import {MatAutocompleteModule} from "@angular/material/autocomplete";
import {MatCardModule} from "@angular/material/card";
import {MatDividerModule} from "@angular/material/divider";
import {MatChipsModule} from "@angular/material/chips";
import {ApiModule, Configuration} from "../../openapi";
import {NavigationModule} from "./components/navigation/navigation.module";
import {MainModule} from "./components/main/main.module";


/**
 * Provides the {@link AppConfig} reference.
 *
 * @param appConfig Reference (provided by DI).
 */
export function initializeApiConfig() {
  return new Configuration({ basePath: "http://localhost:7070", withCredentials: true });
}

@NgModule({
  declarations: [AppComponent],
    imports: [
      {ngModule: ApiModule, providers: [{ provide: Configuration, useFactory: initializeApiConfig }] },

      /* Own modules. */
      AppRoutingModule,
      NavigationModule,
      MainModule,

      /* External modules. */
      BrowserModule,
      BrowserAnimationsModule,
      MatTabsModule,
      MatIconModule,
      DragDropModule,
      HttpClientModule,
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
      MatChipsModule
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
