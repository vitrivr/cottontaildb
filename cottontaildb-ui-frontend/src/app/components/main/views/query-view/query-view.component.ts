import {Component, OnInit} from '@angular/core';
import {FormArray, FormBuilder, FormControl, FormRecord} from "@angular/forms";
import {From, QueryFunction, QueryService, Select} from "../../../../services/query.service";
import {SelectionService} from "../../../../services/selection.service";
import {PageEvent} from "@angular/material/paginator";


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

  queryData: any;
  pageEvent: any;

  constructor(private fb: FormBuilder,
              private queryService: QueryService,
              private selectionService : SelectionService) {
  }

  get queryFunctions(): FormArray {
    return this.queryForm.get("queryFunctions") as FormArray
  }

  ngOnInit(): void {
    this.selectionService.currentSelection.subscribe(selection => this.selection = selection)
    this.addSelect()
  }

  addSelect(){
    this.queryFunctions.push(this.fb.group({'function': new FormControl('select'), 'conditions': new FormRecord({})},))
  }

  addOrder(){
    this.queryFunctions.push(this.fb.group({'function': new FormControl('order'), 'conditions': new FormRecord({})},))
  }

  onLog() {

  }



  onQuery(page: number = 0, pageSize: number = 10) {

    let qm = new Array<QueryFunction>()
    qm.push(new From(this.selection.entity))

    this.queryFunctions.value.forEach((item: any) => {

      switch(item.function) {

        case "select": {
          let columnNames = Object.keys(item.conditions)
          let selected = Object.values(item.conditions)

          if (selected.every(col => col === true)) {
            qm.push(new Select("*"))
          } else if (selected.every(col => col === false)){
            break;
          } else {
            let len = selected.length
            //add new select function call for each selected column
            for (let i = 0; i < len ; i++){
              if (selected[i]){
                qm.push(new Select(columnNames[i]))
              }}
          }
          break;
        }

        default: {
          console.log("undefined queryFunction")
          break;
        }
      }

    })
    this.queryService.query(this.selection.port, this.selection.entity, qm, page, pageSize).subscribe(qd => this.queryData = qd)
  }


  onPageChange($event: PageEvent) {
    let page = $event.pageIndex
    let pageSize = $event.pageSize
    page = page + 1
    this.onQuery(page, pageSize)
  }
}
