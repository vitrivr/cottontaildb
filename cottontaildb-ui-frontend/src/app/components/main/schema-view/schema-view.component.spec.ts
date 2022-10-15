import { ComponentFixture, TestBed } from '@angular/core/testing';

import { SchemaViewComponent } from './schema-view.component';

describe('SchemaViewComponent', () => {
  let component: SchemaViewComponent;
  let fixture: ComponentFixture<SchemaViewComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ SchemaViewComponent ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(SchemaViewComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
