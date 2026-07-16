import { Component, Input, Output, EventEmitter, OnChanges, SimpleChanges } from '@angular/core';
import { VariationTrafic } from '../../models/variation-trafic.model';
import { ParamService } from '../../services/param.service';

@Component({
  selector: 'app-trppu-variation-trafic',
  templateUrl: './trppu-variation-trafic.component.html',
  styleUrls: ['./trppu-variation-trafic.component.css']
})
export class TrppuVariationTraficComponent implements OnChanges {

  @Input() siteId: string | null = null;
  @Output() variationsChange = new EventEmitter<VariationTrafic[]>();

  variations: VariationTrafic[] = [];

  constructor(private paramService: ParamService) {}

  ngOnChanges(changes: SimpleChanges): void {
    if (changes['siteId']) {
      this.loadVariations();
    }
  }

  private loadVariations(): void {
    if (!this.siteId) {
      this.variations = [];
      return;
    }
    this.paramService.getVariationsTrafic(this.siteId).subscribe(data => {
      this.variations = data;
    });
  }

  onSliderChange(variation: VariationTrafic, value: number): void {
    variation.valeur = value;
    this.variationsChange.emit([...this.variations]);
  }
}
