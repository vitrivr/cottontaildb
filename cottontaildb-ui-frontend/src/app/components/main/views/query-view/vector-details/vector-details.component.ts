import {Component, Inject, OnInit} from '@angular/core';
import {MAT_DIALOG_DATA} from '@angular/material/dialog';

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
