import { Component, Inject } from '@angular/core';
import { MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog';

@Component({
  selector: 'app-scenario-validation-dialog',
  templateUrl: './scenario-validation-dialog.component.html',
  styleUrls: ['./scenario-validation-dialog.component.css']
})
export class ScenarioValidationDialogComponent {

  constructor(
    public dialogRef: MatDialogRef<ScenarioValidationDialogComponent>,
    @Inject(MAT_DIALOG_DATA) public data: ScenarioValidationDialogData
  ) {}

  onConfirm(): void {
    this.dialogRef.close(true);
  }

  onCancel(): void {
    this.dialogRef.close(false);
  }
}

export interface ScenarioValidationDialogData {
  message: string;
}
