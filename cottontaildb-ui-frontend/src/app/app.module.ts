import {NgModule} from '@angular/core';
import {BrowserModule} from '@angular/platform-browser';
import {AppComponent} from './app.component';
import {BrowserAnimationsModule} from '@angular/platform-browser/animations';
import {MatLegacyTabsModule as MatTabsModule} from "@angular/material/legacy-tabs";
import {MatIconModule} from '@angular/material/icon';
import {DragDropModule} from "@angular/cdk/drag-drop";
import {HttpClientModule} from '@angular/common/http';
import {AppRoutingModule} from './app-routing.module';
import {MatLegacyTooltipModule as MatTooltipModule} from "@angular/material/legacy-tooltip";
import {MatLegacyButtonModule as MatButtonModule} from "@angular/material/legacy-button";
import {MatLegacyInputModule as MatInputModule} from "@angular/material/legacy-input";
import {FormsModule, ReactiveFormsModule} from "@angular/forms";
import {CreateEntityFormComponent} from './components/navigation/create-entity-form/create-entity-form.component';
import {MatStepperModule} from "@angular/material/stepper";
import {MatLegacySelectModule as MatSelectModule} from "@angular/material/legacy-select";
import {MatLegacyCheckboxModule as MatCheckboxModule} from "@angular/material/legacy-checkbox";
import {MatLegacyTableModule as MatTableModule} from "@angular/material/legacy-table";
import {MatLegacyPaginatorModule as MatPaginatorModule} from '@angular/material/legacy-paginator';
import {MatSortModule} from '@angular/material/sort';
import {MatLegacyMenuModule as MatMenuModule} from "@angular/material/legacy-menu";
import {MatLegacySnackBarModule as MatSnackBarModule} from "@angular/material/legacy-snack-bar";
import {MAT_LEGACY_DIALOG_DATA as MAT_DIALOG_DATA, MatLegacyDialogModule as MatDialogModule, MatLegacyDialogRef as MatDialogRef} from "@angular/material/legacy-dialog";
import {SelectionService} from "./services/selection.service";
import {MatButtonToggleModule} from "@angular/material/button-toggle";
import {MatLegacyProgressSpinnerModule as MatProgressSpinnerModule} from "@angular/material/legacy-progress-spinner";
import {MatLegacyProgressBarModule as MatProgressBarModule} from "@angular/material/legacy-progress-bar";
import {MatLegacyAutocompleteModule as MatAutocompleteModule} from "@angular/material/legacy-autocomplete";
import {MatLegacyCardModule as MatCardModule} from "@angular/material/legacy-card";
import {MatDividerModule} from "@angular/material/divider";
import {MatLegacyChipsModule as MatChipsModule} from "@angular/material/legacy-chips";
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
