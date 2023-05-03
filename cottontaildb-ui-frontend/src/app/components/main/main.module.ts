import {NgModule} from "@angular/core";
import {CommonModule} from "@angular/common";
import {BrowserModule} from "@angular/platform-browser";
import {FormsModule, ReactiveFormsModule} from "@angular/forms";
import {MatTreeModule} from "@angular/material/tree";
import {MatIconModule} from "@angular/material/icon";
import {MatToolbarModule} from "@angular/material/toolbar";
import {MainComponent} from "./main.component";
import {DmlViewComponent} from "./views/dml-view/dml-view.component";
import {QueryViewComponent} from "./views/query-view/query-view.component";
import {SystemViewComponent} from "./views/system-view/system-view.component";
import {CreateIndexFormComponent} from "./views/ddl-view/create-index-form/create-index-form.component";
import {SelectFormComponent} from "./views/query-view/select-form/select-form.component";
import {WhereFormComponent} from "./views/query-view/where-form/where-form.component";
import {OrderFormComponent} from "./views/query-view/order-form/order-form.component";
import {DistanceFormComponent} from "./views/query-view/distance-form/distance-form.component";
import {LimitFormComponent} from "./views/query-view/limit-form/limit-form.component";
import {CountFormComponent} from "./views/query-view/count-form/count-form.component";
import {UseIndexFormComponent} from "./views/query-view/use-index-form/use-index-form.component";
import {UseIndexTypeFormComponent} from "./views/query-view/use-index-type-form/use-index-type-form.component";
import {DisallowParallelismFormComponent} from "./views/query-view/disallow-parallelism-form/disallow-parallelism-form.component";
import {LimitParallelismFormComponent} from "./views/query-view/limit-parallelism-form/limit-parallelism-form.component";
import {DeleteFormComponent} from "./views/dml-view/delete-form/delete-form.component";
import {InsertFormComponent} from "./views/dml-view/insert-form/insert-form.component";
import {UpdateFormComponent} from "./views/dml-view/update-form/update-form.component";
import {VectorDetailsComponent} from "./views/query-view/vector-details/vector-details.component";
import {MatButtonToggleModule} from "@angular/material/button-toggle";
import {MatDividerModule} from "@angular/material/divider";
import {DragDropModule} from "@angular/cdk/drag-drop";
import {MatSortModule} from "@angular/material/sort";
import {ConnectionViewModule} from "./views/connection-view/connection-view-module";
import {EntityViewModule} from "./views/entity-view/entity-view.module";
import {MatCardModule} from "@angular/material/card";
import {MatAutocompleteModule} from "@angular/material/autocomplete";
import {MatButtonModule} from "@angular/material/button";
import {MatFormFieldModule} from "@angular/material/form-field";
import {MatTableModule} from "@angular/material/table";
import {MatInputModule} from "@angular/material/input";
import {MatMenuModule} from "@angular/material/menu";
import {MatOptionModule} from "@angular/material/core";
import {MatTooltipModule} from "@angular/material/tooltip";
import {MatProgressBarModule} from "@angular/material/progress-bar";
import {MatProgressSpinnerModule} from "@angular/material/progress-spinner";
import {MatPaginatorModule} from "@angular/material/paginator";
import {MatTabsModule} from "@angular/material/tabs";
import {MatSnackBarModule} from "@angular/material/snack-bar";

@NgModule({
  declarations: [
    MainComponent,
    DmlViewComponent,
    QueryViewComponent,
    SystemViewComponent,
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
    VectorDetailsComponent
  ],
  imports: [
    /* Internal modules. */
    ConnectionViewModule,
    EntityViewModule,

    /* External modules. */
    BrowserModule,
    CommonModule,
    DragDropModule,
    FormsModule,
    MatAutocompleteModule,
    MatButtonModule,
    MatButtonToggleModule,
    MatCardModule,
    MatDividerModule,
    MatFormFieldModule,
    MatTableModule,
    MatTreeModule,
    MatIconModule,
    MatInputModule,
    MatMenuModule,
    MatOptionModule,
    MatToolbarModule,
    MatTooltipModule,
    MatProgressBarModule,
    MatProgressSpinnerModule,
    MatPaginatorModule,
    MatSnackBarModule,
    ReactiveFormsModule,
    MatTabsModule,
    MatSortModule
  ],
  providers: [MainComponent],
  exports: [MainComponent],
  bootstrap: []
})
export class MainModule {

}
