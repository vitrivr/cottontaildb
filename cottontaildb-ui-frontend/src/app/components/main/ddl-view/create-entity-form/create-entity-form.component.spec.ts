import { ComponentFixture, TestBed } from '@angular/core/testing';

import { CreateEntityFormComponent } from './create-entity-form.component';

describe('CreateEntityFormComponent', () => {
  let component: CreateEntityFormComponent;
  let fixture: ComponentFixture<CreateEntityFormComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ CreateEntityFormComponent ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(CreateEntityFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
