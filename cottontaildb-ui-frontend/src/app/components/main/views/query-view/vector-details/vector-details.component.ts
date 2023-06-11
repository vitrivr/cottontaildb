import {Component, Inject, OnInit} from '@angular/core';
import {MAT_LEGACY_DIALOG_DATA as MAT_DIALOG_DATA} from '@angular/material/legacy-dialog';

@Component({
  selector: 'app-vector-details',
  templateUrl: './vector-details.component.html',
  styleUrls: ['./vector-details.component.css']
})
export class VectorDetailsComponent implements OnInit {

  vector: any;

  constructor(@Inject(MAT_DIALOG_DATA) public data: {vector: string}) {
    this.vector = data.vector
  }

  ngOnInit(): void {
  }


}
