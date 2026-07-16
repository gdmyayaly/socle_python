import { ComponentFixture, TestBed } from '@angular/core/testing';

import { TrppuLoadderComponent } from './trppu-loadder.component';

describe('TrppuLoadderComponent', () => {
  let component: TrppuLoadderComponent;
  let fixture: ComponentFixture<TrppuLoadderComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ TrppuLoadderComponent ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(TrppuLoadderComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
