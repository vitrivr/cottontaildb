import { Component, OnInit } from '@angular/core';
import {ConnectionService} from "../../services/connection.service";

@Component({
  selector: 'app-header',
  templateUrl: './header.component.html',
  styleUrls: ['./header.component.css']
})
export class HeaderComponent implements OnInit {

  connection = false

  constructor(private connectionService: ConnectionService) { }

  ngOnInit(): void {
    this.connectionService.connectionSubject.subscribe({
      next: value => {this.connection = value.length != 0}
    })
  }

}
