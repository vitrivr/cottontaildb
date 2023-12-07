import {Component, Input, OnInit} from "@angular/core";
import {BehaviorSubject, catchError, combineLatestWith, mergeMap, Observable, Subscription} from "rxjs";
import {NavigatedDbo} from "../../navigated-dbo";
import {EntityDetails, EntityService} from "../../../../../../openapi";
import {MatSnackBar, MatSnackBarConfig} from "@angular/material/snack-bar";

@Component({
  selector: 'entity-about',
  templateUrl: './entity-about.component.html',
})
export class EntityAboutComponent implements OnInit {

  @Input() dbo!: Observable<NavigatedDbo>;

  /** A {@link Subscription} reference that is created upon initialization of the view. */
  public data: Observable<EntityDetails>

  /** A {@link BehaviorSubject} that can be used to trigger a manual reload. */
  private _reload = new BehaviorSubject<null>(null);

  /** Flag indicating, that data for this view is currently being loaded. */
  public isLoading: boolean = false

  constructor(private _snackBar: MatSnackBar, private entity: EntityService) {

  }

  ngOnInit(): void {
    this.data = this.dbo.pipe(
      combineLatestWith(this._reload),
      mergeMap(([dbo, reload]) => {
        return this.entity.getEntityAbout(dbo.connection!!, dbo.schema!!, dbo.entity!!).pipe(
          catchError((err) => {
            this._snackBar.open(`Error occurred when trying to load information about entity: ${err.error.description}.`, "Dismiss", { duration: 2000 } as MatSnackBarConfig);
            this.isLoading = false
            return []
          })
        )
      })
    )
  }
}
