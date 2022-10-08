import { ComponentFixture, TestBed } from '@angular/core/testing';

import { DdlViewComponent } from './ddl-view.component';

describe('DdlViewComponent', () => {
  let component: DdlViewComponent;
  let fixture: ComponentFixture<DdlViewComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ DdlViewComponent ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(DdlViewComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
