import { ComponentFixture, TestBed } from '@angular/core/testing';

import { CreateSchemaFormComponent } from './create-schema-form.component';

describe('CreateSchemaFormComponent', () => {
  let component: CreateSchemaFormComponent;
  let fixture: ComponentFixture<CreateSchemaFormComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ CreateSchemaFormComponent ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(CreateSchemaFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
