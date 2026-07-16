import { ComponentFixture, TestBed } from '@angular/core/testing';

import { CalculTrppuComponent } from './calcul-trppu.component';

describe('CalculTrppuComponent', () => {
  let component: CalculTrppuComponent;
  let fixture: ComponentFixture<CalculTrppuComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ CalculTrppuComponent ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(CalculTrppuComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
