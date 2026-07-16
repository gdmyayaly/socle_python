import { ComponentFixture, TestBed } from '@angular/core/testing';

import { TrppuVariationPrevisionnelleComponent } from './trppu-variation-previsionnelle.component';

describe('TrppuVariationPrevisionnelleComponent', () => {
  let component: TrppuVariationPrevisionnelleComponent;
  let fixture: ComponentFixture<TrppuVariationPrevisionnelleComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ TrppuVariationPrevisionnelleComponent ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(TrppuVariationPrevisionnelleComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
