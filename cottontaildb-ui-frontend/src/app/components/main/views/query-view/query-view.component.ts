import {Component, OnInit} from '@angular/core';
import {FormArray, FormBuilder, FormControl, FormRecord} from "@angular/forms";
import {Distance, From, Limit, Order, QueryFunction, QueryService, Select} from "../../../../services/query.service";
import {SelectionService} from "../../../../services/selection.service";
import {PageEvent} from "@angular/material/paginator";
import {CdkDragDrop} from "@angular/cdk/drag-drop";


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
  querying: any;

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

  addDistance(){
    this.queryFunctions.push(this.fb.group({'function': new FormControl('distance'), 'conditions': new FormRecord({})},))
  }

  addLimit(){
    this.queryFunctions.push(this.fb.group({'function': new FormControl('limit'), 'conditions': new FormRecord({})},))
  }


  onQuery(page: number = 0, pageSize: number = 5) {

    this.querying = true

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

        case "order": {
          console.log("ORDER")
          let conditions = Object.values(item.conditions) as Array<string>
          qm.push(new Order(conditions[0], conditions[1]))
          break;
        }

        case "limit": {
          let conditions = Object.values(item.conditions) as Array<number>
          qm.push(new Limit(conditions[0]))
          break;
        }

        case "distance": {
          let conditions = Object.values(item.conditions) as Array<any>
          qm.push(new Distance(conditions[0], conditions[1], conditions[2], conditions[3], conditions[4]))
          break;
        }

        default: {
          console.log("undefined queryFunction")
          break;
        }
      }

    })
    this.queryService.query(this.selection.port, this.selection.entity, qm, page, pageSize).subscribe(qd => {
      this.queryData = qd
      this.querying = false
    })
  }


  onPageChange($event: PageEvent) {
    let page = $event.pageIndex
    let pageSize = $event.pageSize
    this.onQuery(page, pageSize)
  }

  trim(rowElement: string) {
    if(rowElement.length > 30){
      let len = rowElement.length
      return rowElement.slice(0, 10) + "..." + rowElement.slice(len-10, len)
    }
    return rowElement
  }

  moveItemInFormArray(formArray: FormArray, fromIndex: number, toIndex: number): void {
    const dir = toIndex > fromIndex ? 1 : -1;

    const from = fromIndex;
    const to = toIndex;

    const temp = formArray.at(from);
    for (let i = from; i * dir < to * dir; i = i + dir) {
      const current = formArray.at(i + dir);
      formArray.setControl(i, current);
    }
    formArray.setControl(to, temp);
  }

  drop(event: CdkDragDrop<string[]>) {
    this.moveItemInFormArray(this.queryFunctions, event.previousIndex, event.currentIndex);
  }



}
