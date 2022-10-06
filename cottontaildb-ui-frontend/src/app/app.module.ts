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



@NgModule({
  declarations: [
    AppComponent,
    TabsComponent,
    SidebarComponent,
    MainComponent,
    HeaderComponent,
    OverviewComponent
  ],
  imports: [
    BrowserModule,
    BrowserAnimationsModule,
    MatTabsModule
  ],
  providers: [],
  exports: [
    TabsComponent
  ],
  bootstrap: [AppComponent]
})
export class AppModule { }
