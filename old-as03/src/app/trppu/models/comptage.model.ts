import { Trafic } from './trafic.model';

export interface Comptage {
  id: number;
  trafic: Trafic;
  valeur: number;
}
