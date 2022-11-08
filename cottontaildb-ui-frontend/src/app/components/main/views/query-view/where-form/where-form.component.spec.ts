import { ComponentFixture, TestBed } from '@angular/core/testing';

import { WhereFormComponent } from './where-form.component';

describe('WhereFormComponent', () => {
  let component: WhereFormComponent;
  let fixture: ComponentFixture<WhereFormComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ WhereFormComponent ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(WhereFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
