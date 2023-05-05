/** The currently {@link NavigatedDbo}. This is provided by the parent component. */
import {AfterViewInit, Component, Input, OnDestroy, ViewChild} from "@angular/core";
import {NavigatedDbo} from "../../navigated-dbo";
import {MatSort} from "@angular/material/sort";
import {BehaviorSubject, catchError, combineLatestWith, mergeMap, Observable, startWith, Subscription} from "rxjs";
import {DeleteService, DQLService, Resultset, Types} from "../../../../../../openapi";
import {MatTableDataSource} from "@angular/material/table";
import {MatPaginator} from "@angular/material/paginator";
import {MatSnackBar, MatSnackBarConfig} from "@angular/material/snack-bar";
import {EditableTableElement} from "../../../../model/table/editable-table-element";

@Component({
  selector: 'entity-preview',
  templateUrl: './entity-preview.component.html',
})
export class EntityPreviewComponent implements OnDestroy, AfterViewInit {
  @Input() dbo!: Observable<NavigatedDbo>;

  /** A {@link Subscription} reference that is created upon initialization of the view. */
  private _subscription: Subscription | null = null

  /** The {@link MatTableDataSource} used by this {@link ConnectionViewComponent}. */
  public readonly dataSource = new MatTableDataSource<EditableTableElement<object>>()

  /** The columns displayed by the {@link dataSource}. */
  public columns: string[] = [];

  /** The columns displayed by the {@link dataSource}. */
  public columnTypes: Types[] = [];

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
   * @param d
   * @param dql
   */
  constructor(private _snackBar: MatSnackBar, private dml: DeleteService, private dql: DQLService) {}


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
        return this.dql.getEntityPreview(dbo.connection!!, dbo.schema!!, dbo.entity!!, limit, skip).pipe(
          catchError((err) => {
            this._snackBar.open(`Error occurred when trying to load data for entity: ${err.error.description}.`, "Dismiss", { duration: 2000 } as MatSnackBarConfig);
            this.isLoading = false
            return []
          })
        )
      })
    ).subscribe((r: Resultset) => {
      this.columns = r.columns.map(c => c.name)
      this.columnTypes = r.columns.map(c => c.type)
      this.dataSource.data = r.values.map(o => new EditableTableElement(o, false))
      this.total = r.size
      this.columns.push("___actions___")
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
  public delete(element: Array<object>) {
    const request = {
      type: "Compare",
      lexp: {
        type: "Column",
        name: this.columns[0]
      },
      operator: "EQUAL",
      rexp: {
        type: "Literal",
        value: element[0]
      }
    }

    this.dbo.pipe(
      mergeMap( dbo =>
        this.dml.deleteRecord(dbo.connection!!, dbo.schema!!, dbo.entity!!, request).pipe(
          catchError((err) => {
            this._snackBar.open(`Error occurred when trying to delete entry from entity: ${err.error.description}.`, "Dismiss", { duration: 2000 } as MatSnackBarConfig);
            this.isLoading = false
            return []
          })
        )
      )
    ).subscribe()
  }


  /**
   *
   */
  public reload() {
    this._reload.next(null)
  }
}

