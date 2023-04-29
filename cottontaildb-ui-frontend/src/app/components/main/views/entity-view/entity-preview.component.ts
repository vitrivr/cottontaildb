/** The currently {@link NavigatedDbo}. This is provided by the parent component. */
import {Component, Input, ViewChild} from "@angular/core";
import {NavigatedDbo} from "../../navigated-dbo";
import {MatTableDataSource} from "@angular/material/table";
import {SystemService, Transaction} from "../../../../../../openapi";
import {MatPaginator} from "@angular/material/paginator";
import {MatSort} from "@angular/material/sort";
import {MatSnackBar} from "@angular/material/snack-bar";

@Component({
  selector: 'entity-preview',
  templateUrl: './entity-preview.component.html',
})
export class EntityPreviewComponent {
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
}

