import {Component, Input} from "@angular/core";
import {NavigatedDbo} from "../../navigated-dbo";

@Component({
  selector: 'entity-view',
  templateUrl: './entity-view.component.html',
})
export class EntityViewComponent {

  /** The currently {@link EntityViewComponent}. This is provided by the parent component. */
  @Input() dbo!: NavigatedDbo;

}
