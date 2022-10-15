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
import { DmlViewComponent } from './components/main/dml-view/dml-view.component';
import { QueryViewComponent } from './components/main/query-view/query-view.component';
import { SystemViewComponent } from './components/main/system-view/system-view.component';
import { ButtonComponent } from './components/utilities/button/button.component';
import { DropZoneComponent } from './components/utilities/drop-zone/drop-zone.component';
import { HttpClientModule } from '@angular/common/http';
import { AppRoutingModule } from './app-routing.module';
import { SchemaViewComponent } from './components/main/schema-view/schema-view.component';
import { EntityViewComponent } from './components/main/entity-view/entity-view.component';
import { MatTooltipModule } from "@angular/material/tooltip";
import { MatButtonModule } from "@angular/material/button";
import { MatInputModule } from "@angular/material/input";
import { ReactiveFormsModule, FormsModule}  from "@angular/forms";
import { CreateSchemaFormComponent } from './components/main/schema-view/create-schema-form/create-schema-form.component';

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
    EntityViewComponent,
    CreateSchemaFormComponent
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
    FormsModule
  ],
  providers: [],
  exports: [
  ],
  bootstrap: [AppComponent]
})
export class AppModule { }
