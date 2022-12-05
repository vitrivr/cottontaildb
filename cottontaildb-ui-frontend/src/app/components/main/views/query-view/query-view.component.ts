import {Component, OnInit} from '@angular/core';
import {FormArray, FormBuilder, FormControl, FormRecord} from "@angular/forms";
import {
  Count,
  Distance,
  From,
  Limit,
  Order,
  QueryFunction,
  QueryService,
  Select,
  Where
} from "../../../../services/query.service";
import {SelectionService} from "../../../../services/selection.service";
import {PageEvent} from "@angular/material/paginator";
import {CdkDragDrop} from "@angular/cdk/drag-drop";
import {EntityService} from "../../../../services/entity.service";
import {MatDialog} from "@angular/material/dialog";
import {VectorDetailsComponent} from "./vector-details/vector-details.component";
import {MatSnackBar} from "@angular/material/snack-bar";


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
  private aboutEntityData: any;

  constructor(private fb: FormBuilder,
              private queryService: QueryService,
              private selectionService : SelectionService,
              private entityService: EntityService,
              private dialog: MatDialog,
              private snackBar: MatSnackBar) {
  }

  get queryFunctions(): FormArray {
    return this.queryForm.get("queryFunctions") as FormArray
  }

  ngOnInit(): void {
    this.selectionService.currentSelection.subscribe(selection => this.selection = selection)
  }



  pushControl(name: string){
    this.queryFunctions.push(this.fb.group({'function': new FormControl(name), 'conditions': new FormRecord({})},))
  }

  addNNS(){
    this.pushControl("distance")
    this.pushControl("order")
    this.pushControl("limit")
    let distName = this.queryFunctions.at(this.queryFunctions.length-3).get("conditions")?.get("name")
    if(distName != null){
      //TODO
      console.log(this.queryFunctions.at(this.queryFunctions.length-3).get("conditions"))
    }
  }


  onQuery(page: number = 0, pageSize: number = 5) {

    this.entityService.aboutEntitySubject.subscribe(about => {
      this.aboutEntityData = about
    })

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
          break
        }

        case "order": {
          console.log("ORDER")
          let conditions = Object.values(item.conditions) as Array<string>
          qm.push(new Order(conditions[0], conditions[1]))
          break
        }

        case "limit": {
          let conditions = Object.values(item.conditions) as Array<number>
          qm.push(new Limit(conditions[0]))
          break
        }

        case "distance": {
          let conditions = Object.values(item.conditions) as Array<any>
          qm.push(new Distance(conditions[0], conditions[1], conditions[2], conditions[3], conditions[4]))
          break
        }

        case "where": {
          let conditions = Object.values(item.conditions) as Array<any>
          let type = ""
          this.aboutEntityData.forEach((it: { dbo: string, type: string }) =>{
              if (it.dbo == conditions[0]){
                type = it.type as string
              }})
          if(type != "") {
            qm.push(new Where(conditions[0], conditions[1], conditions[2], type))
          } else {
            console.error("column type for where function not determined")
          }
          break
        }

        case "count": {
          qm.push(new Count())
          break
        }

        default: {
          console.log("undefined queryFunction")
          break
        }
      }

    })
    this.queryService.query(this.selection.connection, this.selection.entity, qm, page, pageSize).subscribe(qd => {
      this.queryData = qd
      this.querying = false
    }, error => {
      this.snackBar.open(error, "ok", {duration:2000})
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

  expandEntry(rowElement: any) {
    this.dialog.open<VectorDetailsComponent>(VectorDetailsComponent, {
      width: 'fit-content',
      height: 'fit-content',
      data: {
        vector: rowElement
      }
    });
  }
}
