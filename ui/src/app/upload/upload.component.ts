import { Component, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { Router } from '@angular/router';
import { ApiService } from '../api.service';
import { MatTableModule } from '@angular/material/table';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';

@Component({
  selector: 'app-upload',
  standalone: true,
  imports: [
    CommonModule,
    MatTableModule,
    MatButtonModule,
    MatIconModule,
    MatProgressSpinnerModule
  ],
  template: `
    <div class="upload-container">
      <h2>Upload MD5 Hash File</h2>
      <p>Upload a text file containing MD5 hashes (one per line, 32 hex characters)</p>
      
      <div class="upload-area">
        <input 
          type="file" 
          #fileInput 
          (change)="onFileSelected($event)"
          accept=".txt,.csv"
          style="display: none;"
        />
        <button mat-raised-button color="primary" (click)="fileInput.click()" [disabled]="uploading">
          {{ uploading ? 'Uploading...' : 'Choose File' }}
        </button>
        <span *ngIf="selectedFile" class="file-name">{{ selectedFile.name }}</span>
      </div>

      <div *ngIf="selectedFile && !uploading" class="actions">
        <button mat-raised-button color="accent" (click)="start()">Start Job</button>
      </div>

      <div *ngIf="error" class="error">{{ error }}</div>

      <div class="jobs-table-container" *ngIf="completedJobs.length > 0">
        <h3>Completed Jobs</h3>
        <table mat-table [dataSource]="completedJobs" class="mat-elevation-z8">
          <ng-container matColumnDef="jobId">
            <th mat-header-cell *matHeaderCellDef>Job ID</th>
            <td mat-cell *matCellDef="let job">{{ formatJobId(job.jobId) }}</td>
          </ng-container>

          <ng-container matColumnDef="createdAt">
            <th mat-header-cell *matHeaderCellDef>Created At</th>
            <td mat-cell *matCellDef="let job">{{ formatDate(job.createdAt) }}</td>
          </ng-container>

          <ng-container matColumnDef="totalHashes">
            <th mat-header-cell *matHeaderCellDef>Total Hashes</th>
            <td mat-cell *matCellDef="let job">{{ job.totalHashes }}</td>
          </ng-container>

          <ng-container matColumnDef="foundCount">
            <th mat-header-cell *matHeaderCellDef>Found</th>
            <td mat-cell *matCellDef="let job">{{ job.foundCount }}</td>
          </ng-container>

          <ng-container matColumnDef="actions">
            <th mat-header-cell *matHeaderCellDef>Actions</th>
            <td mat-cell *matCellDef="let job">
              <button mat-icon-button color="primary" (click)="viewJob(job.jobId)" title="View Job">
                <mat-icon>visibility</mat-icon>
              </button>
              <button mat-icon-button color="accent" (click)="downloadResults(job.jobId)" title="Download Results">
                <mat-icon>download</mat-icon>
              </button>
            </td>
          </ng-container>

          <tr mat-header-row *matHeaderRowDef="displayedColumns"></tr>
          <tr mat-row *matRowDef="let row; columns: displayedColumns;"></tr>
        </table>
      </div>

      <div *ngIf="loadingJobs" class="loading">
        <mat-spinner diameter="40"></mat-spinner>
        <span>Loading completed jobs...</span>
      </div>
    </div>
  `,
  styles: [`
    .upload-container {
      max-width: 1000px;
      margin: 0 auto;
      padding: 30px;
      background: #f9f9f9;
      border-radius: 8px;
    }
    h2 {
      margin-top: 0;
    }
    .upload-area {
      margin: 20px 0;
      display: flex;
      align-items: center;
      gap: 10px;
    }
    button {
      padding: 10px 20px;
      background: #007bff;
      color: white;
      border: none;
      border-radius: 4px;
      cursor: pointer;
      font-size: 16px;
    }
    button:disabled {
      background: #ccc;
      cursor: not-allowed;
    }
    button:hover:not(:disabled) {
      background: #0056b3;
    }
    .btn-primary {
      background: #28a745;
    }
    .btn-primary:hover {
      background: #218838;
    }
    .file-name {
      color: #666;
    }
    .actions {
      margin-top: 20px;
    }
    .error {
      color: #dc3545;
      margin-top: 10px;
      padding: 10px;
      background: #f8d7da;
      border-radius: 4px;
    }
    .jobs-table-container {
      margin-top: 40px;
    }
    .jobs-table-container h3 {
      margin-bottom: 20px;
    }
    table {
      width: 100%;
      background: white;
    }
    th.mat-header-cell {
      font-weight: bold;
      background: #f5f5f5;
    }
    .loading {
      display: flex;
      align-items: center;
      gap: 10px;
      margin-top: 20px;
      justify-content: center;
    }
    button.mat-icon-button {
      margin: 0 4px;
    }
  `]
})
export class UploadComponent implements OnInit {
  selectedFile: File | null = null;
  uploading = false;
  error: string | null = null;
  completedJobs: any[] = [];
  loadingJobs = false;
  displayedColumns: string[] = ['jobId', 'createdAt', 'totalHashes', 'foundCount', 'actions'];

  constructor(private api: ApiService, private router: Router) {}

  ngOnInit() {
    this.loadCompletedJobs();
  }

  loadCompletedJobs() {
    this.loadingJobs = true;
    this.api.getJobs().subscribe({
      next: (jobs: any[]) => {
        this.completedJobs = jobs;
        this.loadingJobs = false;
      },
      error: (err) => {
        console.error('Failed to load jobs:', err);
        this.loadingJobs = false;
      }
    });
  }

  formatDate(dateString: string | any): string {
    if (!dateString) return '';
    // Handle both string and Instant format from backend
    let date: Date;
    if (typeof dateString === 'string') {
      date = new Date(dateString);
    } else {
      // If it's an object with seconds/nanos (Instant format)
      date = new Date(dateString);
    }
    return date.toLocaleString();
  }

  formatJobId(jobId: string | any): string {
    if (!jobId) return '';
    // Handle UUID format
    const idStr = typeof jobId === 'string' ? jobId : String(jobId);
    // Show shortened version for display
    return idStr.length > 36 ? idStr.substring(0, 8) + '...' : idStr;
  }

  viewJob(jobId: string | any) {
    const idStr = typeof jobId === 'string' ? jobId : String(jobId);
    this.router.navigate(['/jobs', idStr]);
  }

  downloadResults(jobId: string | any) {
    const idStr = typeof jobId === 'string' ? jobId : String(jobId);
    this.api.downloadResults(idStr).subscribe({
      next: (blob: Blob) => {
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `${idStr}-results.csv`;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
      },
      error: (err) => {
        this.error = 'Failed to download results';
        console.error(err);
      }
    });
  }

  onFileSelected(event: any) {
    const file = event.target.files[0];
    if (file) {
      this.selectedFile = file;
      this.error = null;
    }
  }

  start() {
    if (!this.selectedFile) {
      this.error = 'Please select a file first';
      return;
    }

    this.uploading = true;
    this.error = null;

    this.api.upload(this.selectedFile).subscribe({
      next: (res: any) => {
        const jobId = res.jobId;
        this.router.navigate(['/jobs', jobId]);
        // Reload jobs list after navigation (in case user comes back)
        this.loadCompletedJobs();
      },
      error: (err) => {
        this.uploading = false;
        this.error = err.error?.message || 'Failed to upload file. Please try again.';
      }
    });
  }
}

