// loader.interceptor.ts
import { Injectable } from '@angular/core';
import {
 HttpInterceptor,
 HttpRequest,
 HttpHandler,
 HttpEvent,
} from '@angular/common/http';
import { Observable } from 'rxjs';
import { finalize } from 'rxjs/operators';
import { Router } from '@angular/router';
import { LoaderService } from './loader.service';

@Injectable()
export class LoaderInterceptor implements HttpInterceptor {
 private readonly baseUrl = '/trppu-api';

 constructor(
  private loaderService: LoaderService,
  private router: Router
 ) {}

 intercept(
  req: HttpRequest<any>,
  next: HttpHandler
 ): Observable<HttpEvent<any>> {
  if (!req.url.includes(this.baseUrl)) {
   return next.handle(req);
  }

  // Route figée au moment du déclenchement (on retire query params / fragment)
  const routeKey = this.router.url.split('?')[0].split('#')[0];

  this.loaderService.show(routeKey);
  return next.handle(req).pipe(
   finalize(() => this.loaderService.hide(routeKey))
  );
 }
}