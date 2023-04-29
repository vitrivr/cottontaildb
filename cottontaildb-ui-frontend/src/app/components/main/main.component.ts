import {Component, OnDestroy, OnInit} from '@angular/core';
import {ActivatedRoute, Router} from "@angular/router";
import {NavigatedDbo} from "./navigated-dbo";
import {BehaviorSubject, filter, Observable, Subscription} from "rxjs";

@Component({
  selector: 'app-main',
  templateUrl: './main.component.html',
  styleUrls: ['./main.component.css']
})
export class MainComponent implements OnInit, OnDestroy {
  /** The currently active {@link NavigatedDbo}. Used to update state of views. */
  private _navigated = new BehaviorSubject<NavigatedDbo|null>(null)

  /** */
  private _subscription: Subscription | null = null

  constructor(private router: Router, private activatedRoute: ActivatedRoute) {

  }

  /**
   *
   */
  get navigated(): Observable<NavigatedDbo> {
    return this._navigated.asObservable().pipe(filter(s => s != null)) as Observable<NavigatedDbo>
  }


  ngOnInit(): void {
    this._subscription = this.activatedRoute.queryParamMap.subscribe(p => {
      const connection = p.get("connection")
      const schema = p.get("schema")
      const entity = p.get("entity")
      if (connection) {
        this._navigated.next(new NavigatedDbo(connection, schema, entity))
      }
    })
  }

  ngOnDestroy(): void {
    this._subscription?.unsubscribe()
    this._subscription = null
  }
}
