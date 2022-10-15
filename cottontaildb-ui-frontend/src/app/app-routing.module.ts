import { NgModule } from '@angular/core';
import {Routes, RouterModule} from '@angular/router';

import {DmlViewComponent} from "./components/main/dml-view/dml-view.component";
import {QueryViewComponent} from "./components/main/query-view/query-view.component";
import {SystemViewComponent} from "./components/main/system-view/system-view.component";
import {SchemaViewComponent} from "./components/main/schema-view/schema-view.component";
import {EntityViewComponent} from "./components/main/entity-view/entity-view.component";

const routes: Routes = [
  { path: '', component: SchemaViewComponent },
  { path: 'schema', component: SchemaViewComponent },
  { path: 'entity', component: EntityViewComponent },
  { path: 'dml', component: DmlViewComponent },
  { path: 'query', component: QueryViewComponent },
  { path: 'system', component: SystemViewComponent },
]

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})

export class AppRoutingModule { }
