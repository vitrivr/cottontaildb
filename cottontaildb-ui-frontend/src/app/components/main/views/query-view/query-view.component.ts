import {Component, OnInit} from '@angular/core';
import {FormArray, FormBuilder, FormControl, FormRecord} from "@angular/forms";
import {QueryFunction, QueryMessage, QueryService, Select} from "../../../../services/query.service";
import {SelectionService} from "../../../../services/selection.service";
import {ConnectionService} from "../../../../services/connection.service";



@Component({
  selector: 'app-query-view',
  templateUrl: './query-view.component.html',
  styleUrls: ['./query-view.component.css']
})

export class QueryViewComponent implements OnInit {

  selection: any

  queryForm = this.fb.group({
    queryFunctions: this.fb.array([])
  })

  constructor(private fb: FormBuilder,
              private queryService: QueryService,
              private selectionService : SelectionService) {
  }

  get queryFunctions(): FormArray {
    return this.queryForm.get("queryFunctions") as FormArray
  }

  ngOnInit(): void {
    this.selectionService.currentSelection.subscribe(selection => this.selection = selection)
  }

  addSelect(){
    this.queryFunctions.push(this.fb.group({'function': new FormControl('select'), 'conditions': new FormRecord({})},))
  }

  onLog() {

  }

  onQuery() {
    let qm = new Array<QueryFunction>()
    this.queryFunctions.value.forEach((item: any) => {
      if(item.function === "select"){
        let select = new Select(item.conditions)
        qm.push(select)
      }
      console.log(qm)
    })
    console.log(this.selection.port)
    console.log(this.queryService.query(this.selection.port, this.selection.entity, new QueryMessage(qm)))
  }


}
