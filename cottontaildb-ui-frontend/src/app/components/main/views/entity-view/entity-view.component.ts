import {Component, Input} from "@angular/core";
import {NavigatedDbo} from "../../navigated-dbo";
import {Observable} from "rxjs";

@Component({
  selector: 'entity-view',
  templateUrl: './entity-view.component.html',
})
export class EntityViewComponent {

  /** The currently {@link EntityViewComponent}. This is provided by the parent component. */
  @Input() dbo!: Observable<NavigatedDbo>;



}
