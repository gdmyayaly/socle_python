import { Component, OnInit } from '@angular/core';

@Component({
  selector: 'app-trpp',
  templateUrl: './trpp.component.html',
  styleUrls: ['./trpp.component.scss']
})
export class TrppComponent implements OnInit {

  constructor() { }

  ngOnInit(): void {
    console.log("je suis app-trpp")
  }

}
