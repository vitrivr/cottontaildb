/** The currently {@link NavigatedDbo}. This is provided by the parent component. */
import {AfterViewInit, Component, Input, OnDestroy, ViewChild} from "@angular/core";
import {NavigatedDbo} from "../../navigated-dbo";
import {MatTableDataSource} from "@angular/material/table";
import {MatPaginator} from "@angular/material/paginator";
import {MatSort} from "@angular/material/sort";
import {MatSnackBar, MatSnackBarConfig} from "@angular/material/snack-bar";
import {BehaviorSubject, catchError, combineLatestWith, mergeMap, Observable, startWith, Subscription} from "rxjs";
import {DQLService, Resultset} from "../../../../../../openapi";

@Component({
  selector: 'entity-preview',
  templateUrl: './entity-preview.component.html',
})
export class EntityPreviewComponent implements OnDestroy, AfterViewInit {
  @Input() dbo!: Observable<NavigatedDbo>;

  /** A {@link Subscription} reference that is created upon initialization of the view. */
  private _subscription: Subscription | null = null

  /** The {@link MatTableDataSource} used by this {@link ConnectionViewComponent}. */
  public readonly dataSource = new MatTableDataSource<string>()

  /** The columns displayed by the {@link dataSource}. */
  public columns: string[] = [];

  /** Number of results in the result set. */
  public total: number = 0

  /** Flag indicating, that view is currently being loaded. */
  public isLoading: boolean = false

  /** */
  private _reload = new BehaviorSubject<null>(null);

  /** The {@link MatPaginator} used by this {@link ConnectionViewComponent}. */
  @ViewChild(MatPaginator) paginator: MatPaginator;

  /** */
  @ViewChild(MatSort) sort: MatSort;

  /**
   *
   * @param _snackBar
   * @param dql
   */
  constructor(private _snackBar: MatSnackBar, private dql: DQLService) {
  }

  /**
   * Initializes the data loading logic for {@link EntityPreviewComponent}.
   */
  public ngAfterViewInit() {
    /* Create subscription. */
    this._subscription = this.dbo.pipe(
      combineLatestWith(this.paginator.page.pipe(startWith(null))),
      combineLatestWith(this.sort.sortChange.pipe(startWith(null))),
      mergeMap(([[dbo, page], sort]) => {
        const limit = page?.pageSize != null ? page.pageSize : 20
        const skip = page?.pageIndex != null ? page.pageSize * page.pageIndex : 0

        /* Prepare for data loading. */
        this.dataSource.data = []
        this.columns = []
        this.total = 0
        this.isLoading = true

        /* Start data loading. */
        return this.dql.getEntityPreview(dbo.connection!!, dbo.schema!!, dbo.entity!!, limit, skip)
      }),
      catchError((err) => {
        this._snackBar.open(`Error occurred when trying to load data for entity: ${err.error.description}.`, "Dismiss", { duration: 2000 } as MatSnackBarConfig);
        return []
      })
    ).subscribe((r: Resultset) => {
      this.columns = r.columns.map(c => c.name)
      this.dataSource.data = r.values
      this.total = r.size
      this.isLoading = false
    })
  }

  /**
   * Cleans the ongoing subscription.
   */
  public ngOnDestroy() {
    this._subscription?.unsubscribe()
    this._subscription = null
  }

  /**
   *
   */
  public reload() {
    this._reload.next(null)
  }
}

