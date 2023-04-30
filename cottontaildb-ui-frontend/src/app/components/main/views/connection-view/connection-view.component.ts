import {Component, Input} from "@angular/core";
import {NavigatedDbo} from "../../navigated-dbo";
import {Observable} from "rxjs";

@Component({
  selector: 'connection-view',
  template: `
    <mat-tab-group>
      <mat-tab label="Transactions" matSort>
        <transaction-list [dbo]="this.dbo"></transaction-list>
      </mat-tab>

      <mat-tab label="Locks">

      </mat-tab>
  </mat-tab-group>`
})
export class ConnectionViewComponent {
  /** The currently {@link NavigatedDbo}. This is provided by the parent component. */
  @Input() dbo!: Observable<NavigatedDbo>;
}
