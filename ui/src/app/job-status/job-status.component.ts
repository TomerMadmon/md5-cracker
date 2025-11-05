import { Component, OnInit, OnDestroy, ChangeDetectorRef } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ActivatedRoute, Router } from '@angular/router';
import { ApiService } from '../api.service';

interface JobStatus {
  jobId: string;
  createdAt: string;
  status: string;
  totalHashes: number;
  batchesExpected: number;
  batchesCompleted: number;
  foundCount: number;
}

const BATCH_SIZE = 1000; // Hashes per batch

@Component({
  selector: 'app-job-status',
  standalone: true,
  imports: [CommonModule],
  template: `
    <div class="status-container">
      <h2>Job Status</h2>
      <div class="job-info">
        <p><strong>Job ID:</strong> {{ jobId }}</p>
        <p><strong>Status:</strong> <span [class]="statusClass">{{ status.status || 'Loading...' }}</span></p>
        <p><strong>Total Hashes:</strong> {{ status.totalHashes || 0 | number }}</p>
        <p><strong>Batches:</strong> {{ status.batchesCompleted || 0 }} / {{ status.batchesExpected || 0 }}</p>
        <p><strong>Found:</strong> {{ status.foundCount || 0 | number }}</p>
        <p *ngIf="status.status === 'RUNNING' && candidatesPerSecond > 0" class="performance-info">
          <strong>Processing Rate:</strong> 
          <span class="rate-value">{{ candidatesPerSecond | number:'1.0-1' }}</span> candidates/sec
        </p>
        <p *ngIf="status.status === 'RUNNING' && elapsedTime > 0" class="performance-info">
          <strong>Elapsed Time:</strong> {{ formatElapsedTime(elapsedTime) }}
        </p>
      </div>

      <div class="progress-bar">
        <div 
          class="progress-fill" 
          [style.width]="progressPercent + '%'"
        ></div>
      </div>
      <p class="progress-text">{{ progressPercent }}% Complete ({{ status.batchesCompleted || 0 }} / {{ status.batchesExpected || 0 }} batches)</p>

      <div *ngIf="status.status === 'COMPLETED'" class="actions">
        <button (click)="downloadResults()" class="btn-primary">Download Results</button>
        <button (click)="goHome()" class="btn-secondary">Back to Home</button>
      </div>

      <div class="events" *ngIf="events.length > 0">
        <h3>Events</h3>
        <div *ngFor="let event of events" class="event">
          <span class="event-time">{{ event.time }}</span>
          <span class="event-type">{{ event.type }}</span>
          <span class="event-data">{{ event.data }}</span>
        </div>
      </div>

      <div *ngIf="error" class="error">{{ error }}</div>
    </div>
  `,
  styles: [`
    .status-container {
      max-width: 800px;
      margin: 0 auto;
      padding: 30px;
      background: #f9f9f9;
      border-radius: 8px;
    }
    h2 {
      margin-top: 0;
    }
    .job-info {
      background: white;
      padding: 20px;
      border-radius: 4px;
      margin-bottom: 20px;
    }
    .job-info p {
      margin: 10px 0;
    }
    .status-running {
      color: #007bff;
      font-weight: bold;
    }
    .status-completed {
      color: #28a745;
      font-weight: bold;
    }
    .progress-bar {
      width: 100%;
      height: 30px;
      background: #e0e0e0;
      border-radius: 15px;
      overflow: hidden;
      margin: 20px 0;
    }
    .progress-fill {
      height: 100%;
      background: #007bff;
      transition: width 0.3s ease;
    }
    .progress-text {
      text-align: center;
      margin-top: 10px;
      font-weight: bold;
    }
    .actions {
      margin-top: 20px;
      text-align: center;
    }
    .btn-primary {
      padding: 10px 20px;
      background: #28a745;
      color: white;
      border: none;
      border-radius: 4px;
      cursor: pointer;
      font-size: 16px;
    }
    .btn-primary:hover {
      background: #218838;
    }
    .btn-secondary {
      padding: 10px 20px;
      background: #007bff;
      color: white;
      border: none;
      border-radius: 4px;
      cursor: pointer;
      font-size: 16px;
      margin-left: 10px;
    }
    .btn-secondary:hover {
      background: #0056b3;
    }
    .events {
      margin-top: 30px;
      background: white;
      padding: 20px;
      border-radius: 4px;
    }
    .event {
      padding: 10px;
      border-bottom: 1px solid #eee;
      display: flex;
      gap: 10px;
    }
    .event-time {
      color: #666;
      font-size: 12px;
    }
    .event-type {
      font-weight: bold;
      color: #007bff;
    }
    .error {
      color: #dc3545;
      margin-top: 10px;
      padding: 10px;
      background: #f8d7da;
      border-radius: 4px;
    }
    .performance-info {
      margin: 15px 0;
      padding: 10px;
      background: #e8f4f8;
      border-radius: 4px;
      border-left: 3px solid #007bff;
    }
    .rate-value {
      color: #007bff;
      font-weight: bold;
      font-size: 1.1em;
    }
  `]
})
export class JobStatusComponent implements OnInit, OnDestroy {
  jobId!: string;
  status: JobStatus = {} as JobStatus;
  events: any[] = [];
  error: string | null = null;
  private eventSource: EventSource | null = null;
  private pollInterval: any;
  private rateUpdateInterval: any;

  constructor(
    private api: ApiService, 
    private route: ActivatedRoute, 
    private router: Router,
    private cdr: ChangeDetectorRef
  ) {}

  ngOnInit() {
    this.jobId = this.route.snapshot.paramMap.get('id')!;
    this.loadStatus();
    this.startEventStream();
    // Update rate calculation every second for real-time updates
    this.rateUpdateInterval = setInterval(() => {
      // Force change detection to update rate display
      this.cdr.detectChanges();
    }, 1000);
  }

  ngOnDestroy() {
    if (this.eventSource) {
      this.eventSource.close();
    }
    if (this.pollInterval) {
      clearInterval(this.pollInterval);
    }
    if (this.rateUpdateInterval) {
      clearInterval(this.rateUpdateInterval);
    }
  }

  loadStatus() {
    this.api.getJob(this.jobId).subscribe({
      next: (data: any) => {
        // Ensure numeric values are properly converted
        this.status = {
          jobId: data.jobId || this.jobId,
          createdAt: data.createdAt,
          status: data.status || 'RUNNING',
          totalHashes: Number(data.totalHashes) || 0,
          batchesExpected: Number(data.batchesExpected) || 0,
          batchesCompleted: Number(data.batchesCompleted) || 0,
          foundCount: Number(data.foundCount) || 0
        };
        console.log('Status updated:', this.status, 'Progress:', this.progressPercent + '%');
      },
      error: (err) => {
        this.error = 'Failed to load job status';
        console.error(err);
      }
    });
  }

  startEventStream() {
    // Try SSE first
    try {
      this.eventSource = new EventSource(`/api/jobs/${this.jobId}/events`);
      this.eventSource.onmessage = (e: any) => {
        try {
          const data = JSON.parse(e.data);
          this.events.push({
            time: new Date().toLocaleTimeString(),
            type: data.type,
            data: JSON.stringify(data.payload)
          });
          
          // Update status immediately from event payload if available
          if (data.type === 'progress' && data.payload) {
            if (data.payload.batchesCompleted !== undefined) {
              this.status.batchesCompleted = Number(data.payload.batchesCompleted) || 0;
            }
            if (data.payload.batchesExpected !== undefined) {
              this.status.batchesExpected = Number(data.payload.batchesExpected) || 0;
            }
            if (data.payload.foundCount !== undefined) {
              this.status.foundCount = Number(data.payload.foundCount) || 0;
            }
          }
          
          // Always reload full status to ensure consistency
          if (data.type === 'progress' || data.type === 'completed') {
            this.loadStatus();
          }
        } catch (err) {
          console.error('Error parsing event:', err);
        }
      };
      this.eventSource.onerror = () => {
        // Fallback to polling if SSE fails
        this.eventSource?.close();
        this.startPolling();
      };
    } catch (e) {
      this.startPolling();
    }
  }

  startPolling() {
    this.pollInterval = setInterval(() => {
      this.loadStatus();
    }, 1000);
  }

  get progressPercent(): number {
    const expected = Number(this.status.batchesExpected) || 0;
    const completed = Number(this.status.batchesCompleted) || 0;
    
    if (!expected || expected === 0) {
      return 0;
    }
    
    const percent = Math.round((completed / expected) * 100);
    // Ensure progress doesn't exceed 100%
    return Math.min(percent, 100);
  }

  get statusClass(): string {
    return `status-${this.status.status?.toLowerCase() || 'running'}`;
  }

  get candidatesPerSecond(): number {
    if (!this.status.createdAt || this.status.status !== 'RUNNING') {
      return 0;
    }
    
    const candidatesProcessed = this.getCandidatesProcessed();
    const elapsed = this.getElapsedSeconds();
    
    if (elapsed <= 0) {
      return 0;
    }
    
    return candidatesProcessed / elapsed;
  }

  get elapsedTime(): number {
    if (!this.status.createdAt) {
      return 0;
    }
    return this.getElapsedSeconds();
  }

  private getCandidatesProcessed(): number {
    const batchesCompleted = Number(this.status.batchesCompleted) || 0;
    const batchesExpected = Number(this.status.batchesExpected) || 0;
    const totalHashes = Number(this.status.totalHashes) || 0;
    
    if (batchesExpected === 0) {
      return 0;
    }
    
    // Calculate candidates per batch (handles last batch being smaller)
    const candidatesPerBatch = totalHashes / batchesExpected;
    return batchesCompleted * candidatesPerBatch;
  }

  private getElapsedSeconds(): number {
    if (!this.status.createdAt) {
      return 0;
    }
    
    const createdAt = new Date(this.status.createdAt);
    const now = new Date();
    const elapsedMs = now.getTime() - createdAt.getTime();
    return elapsedMs / 1000; // Convert to seconds
  }

  formatElapsedTime(seconds: number): string {
    if (seconds < 60) {
      return `${Math.floor(seconds)}s`;
    } else if (seconds < 3600) {
      const mins = Math.floor(seconds / 60);
      const secs = Math.floor(seconds % 60);
      return `${mins}m ${secs}s`;
    } else {
      const hours = Math.floor(seconds / 3600);
      const mins = Math.floor((seconds % 3600) / 60);
      return `${hours}h ${mins}m`;
    }
  }

  downloadResults() {
    this.api.downloadResults(this.jobId).subscribe({
      next: (blob: Blob) => {
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `${this.jobId}-results.csv`;
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

  goHome() {
    this.router.navigate(['/']);
  }
}

