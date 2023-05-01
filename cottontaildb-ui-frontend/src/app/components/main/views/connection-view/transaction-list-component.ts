import {AfterViewInit, Component, Input, OnDestroy, OnInit, ViewChild} from "@angular/core";
import {SystemService, Transaction} from "../../../../../../openapi";
import {NavigatedDbo} from "../../navigated-dbo";
import {BehaviorSubject, catchError, mergeMap, Observable, Subscription} from "rxjs";
import {MatSort} from "@angular/material/sort";
import {MatTableDataSource} from "@angular/material/table";
import {MatPaginator} from "@angular/material/paginator";
import {MatSnackBar, MatSnackBarConfig} from "@angular/material/snack-bar";

@Component({
  selector: 'transaction-list',
  templateUrl: './transaction-list-component.html',
})
export class TransactionListComponent implements OnInit, AfterViewInit, OnDestroy {

  /** The currently {@link NavigatedDbo}. This is provided by the parent component. */
  @Input() dbo!: Observable<NavigatedDbo>;

  /** A {@link BehaviorSubject} that triggers a manual reload. */
  private _reload = new BehaviorSubject<null>(null)

  /** A {@link Subscription} reference that is created upon initialization of the view. */
  private _subscription: Subscription | null = null

  /** The {@link MatTableDataSource} used by this {@link ConnectionViewComponent}. */
  public readonly dataSource = new MatTableDataSource<Transaction>()

  /** The columns displayed by the {@link dataSource}. */
  public readonly columns: string[] = ['txId', 'type', 'state', 'created', 'ended', 'duration', 'action'];

  /** The {@link MatPaginator} used by this {@link ConnectionViewComponent}. */
  @ViewChild(MatPaginator) paginator: MatPaginator;

  /** */
  @ViewChild(MatSort) sort: MatSort;

  /**
   *
   * @param _snackBar
   * @param system
   */
  constructor(private _snackBar: MatSnackBar, private system: SystemService) {
  }

  /**
   * Initializes the data loading logic for {@link TransactionListComponent}.
   */
  public ngOnInit(): void {
    /* Create observable. */
    this._subscription = this.dbo.pipe(
      mergeMap((dbo) => {
        return this.system.getListTransactions(dbo.connection)
      }),
      catchError((err) => {
        this._snackBar.open(`Error occurred when trying to load transaction: ${err.error.description}.`, "Dismiss", { duration: 2000 } as MatSnackBarConfig);
        return [];
      })).subscribe((r) => {
        this.dataSource.data = r
      })
  }

  /**
   * Cleans the ongoing subscription.
   */
  public ngOnDestroy() {
    this._subscription?.unsubscribe()
    this._subscription = null
  }

  public ngAfterViewInit(): void {
    this.dataSource.paginator = this.paginator;
    this.dataSource.sort = this.sort;
    this.sort.direction = 'desc'
    this.sort.active = 'txId'
  }

  /**
   * Reloads all the data required for this {@link ConnectionViewComponent}.
   */
  public reload() {
    this._reload.next(null)
  }

  /**
   * Attempts to kill the selected transaction.
   *
   * @param txId ID of the transaction.
   */
  public killTransaction(connection: string, txId: number) {
    if (window.confirm(`Are you sure you want to kill transaction ${txId}? This can cause unexpected side-effects!`)) {
      this.system.deleteKillTransaction(connection, txId.toString()).pipe(
        catchError((err) => {
          this._snackBar.open(`Error occurred upon killing transaction ${txId}: ${err.error.description}.`, "Dismiss", { duration: 2000 } as MatSnackBarConfig);
          return []
        })).subscribe(r => {
          this._snackBar.open(`Transaction ${txId} killed successfully`);
          this.reload()
      })
    }
  }
}
