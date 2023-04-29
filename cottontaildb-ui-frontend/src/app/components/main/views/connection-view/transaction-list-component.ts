import {AfterViewInit, Component, Input, OnInit, ViewChild} from "@angular/core";
import {SystemService, Transaction} from "../../../../../../openapi";
import {NavigatedDbo} from "../../navigated-dbo";
import {DboType} from "../../../../model/dbo/dbo-type";
import {catchError} from "rxjs";
import {MatSnackBar, MatSnackBarConfig} from "@angular/material/snack-bar";
import {MatPaginator} from "@angular/material/paginator";
import {MatTableDataSource} from "@angular/material/table";
import {MatSort} from "@angular/material/sort";

@Component({
  selector: 'transaction-list',
  templateUrl: './transaction-list-component.html',
})
export class TransactionListComponent implements OnInit, AfterViewInit {

  /** The currently {@link NavigatedDbo}. This is provided by the parent component. */
  @Input() dbo!: NavigatedDbo;

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
   *
   */
  public ngOnInit(): void {
    if (this.dbo?.type == DboType.CONNECTION) {
      this.reload()
    }
  }

  /**
   * Reloads all the data required for this {@link ConnectionViewComponent}.
   */
  public reload() {
    this.system.getListTransactions(this.dbo!!.connection!!).pipe(
      catchError((err) => {
        this._snackBar.open(`Error occurred when trying to load transaction: ${err.error.description}.`, "Dismiss", { duration: 2000 } as MatSnackBarConfig);
        return []
      })).subscribe(r => {
      this.dataSource.data = r
    })
  }

  /**
   * Attempts to kill the selected transaction.
   *
   * @param txId ID of the transaction.
   */
  public killTransaction(txId: number) {
    if (window.confirm(`Are you sure you want to kill transaction ${txId}? This can cause unexpected side-effects!`)) {
      this.system.deleteKillTransaction(this.dbo!!.connection, txId.toString()).pipe(
        catchError((err) => {
          this._snackBar.open(`Error occurred upon killing transaction ${txId}: ${err.error.description}.`, "Dismiss", { duration: 2000 } as MatSnackBarConfig);
          return []
        })).subscribe(r => {
          this._snackBar.open(`Transaction ${txId} killed successfully`);
          this.reload()
      })
    }
  }

  ngAfterViewInit(): void {
    this.dataSource.paginator = this.paginator;
    this.dataSource.sort = this.sort;
    this.sort.direction = 'desc'
    this.sort.active = 'txId'
  }
}
