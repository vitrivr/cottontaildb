import {Component, OnDestroy, OnInit} from '@angular/core';
import {ActivatedRoute, Router} from "@angular/router";
import {NavigatedDbo} from "./navigated-dbo";

@Component({
  selector: 'app-main',
  templateUrl: './main.component.html',
  styleUrls: ['./main.component.css']
})
export class MainComponent implements OnInit, OnDestroy {

  public navigated: NavigatedDbo | undefined = undefined

  constructor(private router: Router, private activatedRoute: ActivatedRoute) {

  }

  ngOnInit(): void {
    this.activatedRoute.queryParamMap.subscribe(p => {
      const connection = p.get("connection")
      const schema = p.get("schema")
      const entity = p.get("entity")
      if (connection) {
        this.navigated = new NavigatedDbo(connection, schema, entity)
      }
    })
  }

  ngOnDestroy(): void {
  }
}
