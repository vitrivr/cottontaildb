import {Component, Input, OnInit} from '@angular/core';
import {FormGroupDirective} from "@angular/forms";

@Component({
  selector: 'app-count-form',
  templateUrl: './count-form.component.html',
  styleUrls: ['./count-form.component.css']
})
export class CountFormComponent implements OnInit {

  @Input() index!: number;

  conditions: any;
  form: any

  constructor(private rootFormGroup: FormGroupDirective) { }


  ngOnInit(): void {
    this.form = this.rootFormGroup.control

  }


  remove() {
    this.form.get("queryFunctions").removeAt(this.index)
  }

}
