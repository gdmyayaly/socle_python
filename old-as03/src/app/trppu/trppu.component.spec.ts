import { ComponentFixture, TestBed } from '@angular/core/testing';

import { TrppuComponent } from './trppu.component';

describe('TrppuComponent', () => {
  let component: TrppuComponent;
  let fixture: ComponentFixture<TrppuComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ TrppuComponent ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(TrppuComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
