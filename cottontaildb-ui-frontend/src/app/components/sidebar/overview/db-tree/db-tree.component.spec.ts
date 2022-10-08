import { ComponentFixture, TestBed } from '@angular/core/testing';

import { DbTreeComponent } from './db-tree.component';

describe('DbTreeComponent', () => {
  let component: DbTreeComponent;
  let fixture: ComponentFixture<DbTreeComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ DbTreeComponent ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(DbTreeComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
