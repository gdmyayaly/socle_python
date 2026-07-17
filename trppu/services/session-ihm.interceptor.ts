import { Injectable } from '@angular/core';
import {
  HttpInterceptor,
  HttpRequest,
  HttpHandler,
  HttpEvent,
} from '@angular/common/http';
import { Observable } from 'rxjs';
import { TrppuContextService } from './trppu-context.service';

/**
 * Ajoute l'identifiant de session IHM (`id_session_ihm`) en query param sur
 * tous les appels au micro-service TRPPU, pour le regroupement des logs par
 * session dans Kibana (exigence DSR-660/661 : tous les logs doivent porter
 * l'id de session IHM).
 *
 * - L'UUID provient de `TrppuContextService.getOrCreateIdSession()` ; il est
 *   rafraîchi à chaque connexion par l'application hôte (generateIdSession).
 * - Si un appel pose déjà le param lui-même (ex. PicCoefficientService),
 *   l'interceptor ne le double pas.
 * - Le backend l'accepte en query optionnel : les routes qui ne le déclarent
 *   pas l'ignorent, et le handler 422 global le relit depuis la query string.
 */
@Injectable()
export class SessionIhmInterceptor implements HttpInterceptor {
  private readonly baseUrl = '/trppu-api';

  constructor(private context: TrppuContextService) {}

  intercept(
    req: HttpRequest<any>,
    next: HttpHandler
  ): Observable<HttpEvent<any>> {
    if (!req.url.includes(this.baseUrl)) {
      return next.handle(req);
    }

    if (req.params.has('id_session_ihm')) {
      return next.handle(req);
    }

    return next.handle(
      req.clone({
        params: req.params.set('id_session_ihm', this.context.getOrCreateIdSession()),
      })
    );
  }
}
