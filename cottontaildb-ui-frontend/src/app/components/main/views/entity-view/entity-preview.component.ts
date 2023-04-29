/** The currently {@link NavigatedDbo}. This is provided by the parent component. */
import {AfterViewInit, Component, Input, ViewChild} from "@angular/core";
import {NavigatedDbo} from "../../navigated-dbo";
import {MatTableDataSource} from "@angular/material/table";
import {MatPaginator, PageEvent} from "@angular/material/paginator";
import {MatSort} from "@angular/material/sort";
import {MatSnackBar, MatSnackBarConfig} from "@angular/material/snack-bar";
import {BehaviorSubject, catchError, combineLatestWith, mergeMap} from "rxjs";
import {DQLService, Resultset} from "../../../../../../openapi";

@Component({
  selector: 'entity-preview',
  templateUrl: './entity-preview.component.html',
})
export class EntityPreviewComponent implements AfterViewInit {
  @Input() dbo!: NavigatedDbo;

  /** The {@link MatTableDataSource} used by this {@link ConnectionViewComponent}. */
  public readonly dataSource = new MatTableDataSource<string>()

  /** The columns displayed by the {@link dataSource}. */
  public columns: string[] = [];

  /** */
  private _reload = new BehaviorSubject<null>(null);

  /** The {@link MatPaginator} used by this {@link ConnectionViewComponent}. */
  @ViewChild(MatPaginator) paginator: MatPaginator;

  /** */
  @ViewChild(MatSort) sort: MatSort;

  /**
   *
   * @param _snackBar
   * @param system
   */
  constructor(private _snackBar: MatSnackBar, private dql: DQLService) {
  }

  /**
   *
   */
  public ngAfterViewInit(): void {
    this.dataSource.paginator = this.paginator;
    this.dataSource.sort = this.sort;

    this._reload.pipe(
      combineLatestWith(this.paginator.page),
      combineLatestWith(this.sort.sortChange),
      mergeMap((t) => {
        const limit = (<PageEvent>t[0][1]).pageSize
        const skip = (<PageEvent>t[0][1]).pageSize * (<PageEvent>t[0][1]).pageIndex
        return this.dql.getEntityPreview(this.dbo!!.connection!!, this.dbo!!.schema!!, this.dbo!!.entity!!, limit, skip).pipe(
            catchError((err) => {
              this._snackBar.open(`Error occurred when trying to load data for entity: ${err.error.description}.`, "Dismiss", { duration: 2000 } as MatSnackBarConfig);
              return []
            })
        )
      })
    ).subscribe((r: Resultset) => {
      this.columns = r.columns.map(c => c.name)
      this.dataSource.data = r.values
    })

    this.reload()
  }

  /**
   *
   */
  public reload() {
    this._reload.next(null)
  }
}

