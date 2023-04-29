import {Component, Inject, OnInit} from '@angular/core';
import {FormControl, FormGroup, Validators} from "@angular/forms";
import {EntityService} from "../../../../../services/entity.service";
import {MAT_DIALOG_DATA, MatDialogRef} from "@angular/material/dialog";
import {MatSnackBar} from "@angular/material/snack-bar";
import {SelectionService} from "../../../../../services/selection.service";
import {CreateEntityFormComponent} from "../../../../navigation/create-entity-form/create-entity-form.component";
import {Connection} from "../../../../../../../openapi";

@Component({
  selector: 'app-create-index-form',
  templateUrl: './create-index-form.component.html',
  styleUrls: ['./create-index-form.component.css']
})
export class CreateIndexFormComponent implements OnInit {

  indexDef: any
  selection: any
  loading: boolean = false

  constructor(@Inject(MAT_DIALOG_DATA) public data: {dbo: string, connection: Connection},
              private entityService: EntityService,
              private snackbar: MatSnackBar,
              private selectionService: SelectionService,
              private dialogRef: MatDialogRef<CreateEntityFormComponent>) {
    this.indexDef = []
  }

  ngOnInit(): void {
    this.loading = false
    this.selectionService.currentSelection.subscribe(selection => this.selection = selection)
  }

  indexForm = new FormGroup({
    index: new FormControl('', [Validators.required]),
    skipBuild: new FormControl('', [Validators.required])
  });

  indexTypes: String[] = ['BTREE','BTREE_UQ','LUCENE','VAF','PQ','LSH'];

  sumbmitIndexForm() {
    // wrapping the form data in an array makes it okay for the types to be unknown...
    // (FormControl is not statically typed...)
    this.indexDef = (this.indexForm.value)
    this.loading = true;
    this.entityService.createIndex(this.data.connection, this.data.dbo, this.indexDef).subscribe({
      next: () => {
        this.dialogRef.close()
        this.snackbar.open("successfully created index", "ok", {duration:2000})
      },
      error: err => this.snackbar.open(err, "ok", {duration:2000})
    })

  }
}
