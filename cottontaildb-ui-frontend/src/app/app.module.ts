import { NgModule } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';
import { AppComponent } from './app.component';
import { TabsComponent } from './components/main/tabs/tabs.component';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { MainComponent } from './components/main/main.component';
import { MatTabsModule} from "@angular/material/tabs";
import { SidebarComponent} from "./components/sidebar/sidebar.component";
import { HeaderComponent } from './components/main/header/header.component';
import { OverviewComponent } from './components/sidebar/overview/overview.component';
import { DbTreeComponent } from './components/sidebar/overview/db-tree/db-tree.component';
import { TreeComponent } from './components/tree/tree.component';
import { MatTreeModule } from '@angular/material/tree';
import {MatIcon, MatIconModule} from '@angular/material/icon';
import {DragDropModule} from "@angular/cdk/drag-drop";
import { DdlViewComponent } from './components/main/tabs/ddl-view/ddl-view.component';
import { DmlViewComponent } from './components/main/tabs/dml-view/dml-view.component';
import { QueryViewComponent } from './components/main/tabs/query-view/query-view.component';
import { SystemViewComponent } from './components/main/tabs/system-view/system-view.component';
import { ButtonComponent } from './components/button/button.component';
import { DropZoneComponent } from './components/drop-zone/drop-zone.component';
import { HttpClientModule } from '@angular/common/http'





@NgModule({
  declarations: [
    AppComponent,
    TabsComponent,
    SidebarComponent,
    MainComponent,
    HeaderComponent,
    OverviewComponent,
    DbTreeComponent,
    TreeComponent,
    DdlViewComponent,
    DmlViewComponent,
    QueryViewComponent,
    SystemViewComponent,
    ButtonComponent,
    DropZoneComponent
  ],
  imports: [
    BrowserModule,
    BrowserAnimationsModule,
    MatTabsModule,
    MatTreeModule,
    MatIconModule,
    DragDropModule,
    HttpClientModule
  ],
  providers: [],
  exports: [
    TabsComponent
  ],
  bootstrap: [AppComponent]
})
export class AppModule { }
