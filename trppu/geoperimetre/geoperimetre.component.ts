import { Component, OnInit } from '@angular/core';
import { UserService } from '../../../service/user/user.service';

@Component({
  selector: 'app-geoperimetre',
  templateUrl: './geoperimetre.component.html',
  styleUrls: ['./geoperimetre.component.scss']
})
export class GeoperimetreComponent implements OnInit {

  constructor(private userService: UserService) { }

  ngOnInit(): void {
  }

  public get user() {
    return this.userService;
  }

  public get roles() {
    return this.userService.user.roles;
  }
}
