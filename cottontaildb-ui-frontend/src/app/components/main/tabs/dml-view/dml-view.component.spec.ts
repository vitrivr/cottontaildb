import { ComponentFixture, TestBed } from '@angular/core/testing';

import { DmlViewComponent } from './dml-view.component';

describe('DmlViewComponent', () => {
  let component: DmlViewComponent;
  let fixture: ComponentFixture<DmlViewComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ DmlViewComponent ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(DmlViewComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
