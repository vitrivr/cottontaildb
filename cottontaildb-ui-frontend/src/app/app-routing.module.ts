import { NgModule } from '@angular/core';
import {Routes, RouterModule} from '@angular/router';

import {DmlViewComponent} from "./components/main/views/dml-view/dml-view.component";
import {QueryViewComponent} from "./components/main/views/query-view/query-view.component";
import {SystemViewComponent} from "./components/main/views/system-view/system-view.component";
import {DdlViewComponent} from "./components/main/views/ddl-view/ddl-view.component";

const routes: Routes = [
  { path: '', component: DdlViewComponent },
  { path: 'ddl', component: DdlViewComponent },
  { path: 'dml', component: DmlViewComponent },
  { path: 'query', component: QueryViewComponent },
  { path: 'system', component: SystemViewComponent },
]

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})

export class AppRoutingModule { }
