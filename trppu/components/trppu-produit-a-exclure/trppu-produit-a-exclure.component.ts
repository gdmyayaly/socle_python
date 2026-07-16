import { Component, Input, Output, EventEmitter, OnChanges, SimpleChanges } from '@angular/core';
import { ProduitExclu } from '../../models/produit-exclu.model';
import { ParamService } from '../../services/param.service';

const DEFAULT_PRODUITS: ProduitExclu[] = [
  { id: 1, code: 'P01', libelle: 'Produit 01', exclu: false },
  { id: 2, code: 'P02', libelle: 'Produit 02', exclu: false },
  { id: 3, code: 'P03', libelle: 'Produit 03', exclu: false },
  { id: 4, code: 'P04', libelle: 'Produit 04', exclu: false },
  { id: 5, code: 'P05', libelle: 'Produit 05', exclu: false }
];

@Component({
  selector: 'app-trppu-produit-a-exclure',
  templateUrl: './trppu-produit-a-exclure.component.html',
  styleUrls: ['./trppu-produit-a-exclure.component.css']
})
export class TrppuProduitAExclureComponent implements OnChanges {

  @Input() siteId: string | null = null;
  @Input() produits: ProduitExclu[] | null = null;

  @Output() produitsChange = new EventEmitter<ProduitExclu[]>();

  resolvedProduits: ProduitExclu[] = [];

  constructor(private paramService: ParamService) {
    this.resolveProduits();
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (changes['produits']) {
      this.resolveProduits();
    }
    if (changes['siteId']) {
      this.loadProduits();
    }
  }

  private resolveProduits(): void {
    if (this.produits && this.produits.length > 0) {
      this.resolvedProduits = this.produits.map(p => ({ ...p }));
    } else {
      this.resolvedProduits = DEFAULT_PRODUITS.map(p => ({ ...p }));
    }
  }

  private loadProduits(): void {
    if (!this.siteId) {
      this.resolveProduits();
      return;
    }
    this.paramService.getProduitsAExclure(this.siteId).subscribe({
      next: (data) => {
        if (data && data.length > 0) {
          this.resolvedProduits = data;
        } else {
          this.resolveProduits();
        }
      },
      error: () => this.resolveProduits()
    });
  }

  onToggle(_produit: ProduitExclu): void {
    this.produitsChange.emit([...this.resolvedProduits]);
  }
}
