import { Component, Input, Output, EventEmitter, OnChanges, SimpleChanges } from '@angular/core';
import { ProduitExclu } from '../../models/produit-exclu.model';
import { ParamService } from '../../services/param.service';

@Component({
  selector: 'app-trppu-produit-a-exclure',
  templateUrl: './trppu-produit-a-exclure.component.html',
  styleUrls: ['./trppu-produit-a-exclure.component.css']
})
export class TrppuProduitAExclureComponent implements OnChanges {

  @Input() siteId: string | null = null;
  @Output() produitsChange = new EventEmitter<ProduitExclu[]>();

  produits: ProduitExclu[] = [];

  constructor(private paramService: ParamService) {}

  ngOnChanges(changes: SimpleChanges): void {
    if (changes['siteId']) {
      this.loadProduits();
    }
  }

  private loadProduits(): void {
    if (!this.siteId) {
      this.produits = [];
      return;
    }
    this.paramService.getProduitsAExclure(this.siteId).subscribe(data => {
      this.produits = data;
    });
  }

  onToggle(produit: ProduitExclu): void {
    this.produitsChange.emit([...this.produits]);
  }
}
