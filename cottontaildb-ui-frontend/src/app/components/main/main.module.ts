import {NgModule} from "@angular/core";
import {CommonModule} from "@angular/common";
import {BrowserModule} from "@angular/platform-browser";
import {FormsModule, ReactiveFormsModule} from "@angular/forms";
import {MatButtonModule} from "@angular/material/button";
import {MatFormFieldModule} from "@angular/material/form-field";
import {MatTableModule} from "@angular/material/table";
import {MatTreeModule} from "@angular/material/tree";
import {MatIconModule} from "@angular/material/icon";
import {MatInputModule} from "@angular/material/input";
import {MatMenuModule} from "@angular/material/menu";
import {MatOptionModule} from "@angular/material/core";
import {MatToolbarModule} from "@angular/material/toolbar";
import {MatProgressBarModule} from "@angular/material/progress-bar";
import {MatProgressSpinnerModule} from "@angular/material/progress-spinner";
import {MainComponent} from "./main.component";
import {DmlViewComponent} from "./views/dml-view/dml-view.component";
import {QueryViewComponent} from "./views/query-view/query-view.component";
import {SystemViewComponent} from "./views/system-view/system-view.component";
import {DdlViewComponent} from "./views/ddl-view/ddl-view.component";
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
import {MatAutocompleteModule} from "@angular/material/autocomplete";
import {MatButtonToggleModule} from "@angular/material/button-toggle";
import {MatTooltipModule} from "@angular/material/tooltip";
import {MatCardModule} from "@angular/material/card";
import {MatDividerModule} from "@angular/material/divider";
import {MatPaginatorModule} from "@angular/material/paginator";
import {DragDropModule} from "@angular/cdk/drag-drop";

@NgModule({
  declarations: [
    MainComponent,
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
    VectorDetailsComponent
  ],
  imports: [
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
    ReactiveFormsModule
  ],
  providers: [MainComponent],
  exports: [MainComponent],
  bootstrap: []
})
export class MainModule {

}
