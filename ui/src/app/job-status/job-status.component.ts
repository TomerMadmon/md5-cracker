import { Component, OnInit, OnDestroy } from '@angular/core';
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
        <p><strong>Total Hashes:</strong> {{ status.totalHashes || 0 }}</p>
        <p><strong>Batches:</strong> {{ status.batchesCompleted || 0 }} / {{ status.batchesExpected || 0 }}</p>
        <p><strong>Found:</strong> {{ status.foundCount || 0 }}</p>
      </div>

      <div class="progress-bar">
        <div 
          class="progress-fill" 
          [style.width.%]="progressPercent"
        ></div>
      </div>
      <p class="progress-text">{{ progressPercent }}% Complete</p>

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
  `]
})
export class JobStatusComponent implements OnInit, OnDestroy {
  jobId!: string;
  status: JobStatus = {} as JobStatus;
  events: any[] = [];
  error: string | null = null;
  private eventSource: EventSource | null = null;
  private pollInterval: any;

  constructor(private api: ApiService, private route: ActivatedRoute, private router: Router) {}

  ngOnInit() {
    this.jobId = this.route.snapshot.paramMap.get('id')!;
    this.loadStatus();
    this.startEventStream();
  }

  ngOnDestroy() {
    if (this.eventSource) {
      this.eventSource.close();
    }
    if (this.pollInterval) {
      clearInterval(this.pollInterval);
    }
  }

  loadStatus() {
    this.api.getJob(this.jobId).subscribe({
      next: (data: any) => {
        this.status = {
          jobId: data.jobId || this.jobId,
          createdAt: data.createdAt,
          status: data.status || 'RUNNING',
          totalHashes: data.totalHashes || 0,
          batchesExpected: data.batchesExpected || 0,
          batchesCompleted: data.batchesCompleted || 0,
          foundCount: data.foundCount || 0
        };
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
        const data = JSON.parse(e.data);
        this.events.push({
          time: new Date().toLocaleTimeString(),
          type: data.type,
          data: JSON.stringify(data.payload)
        });
        
        if (data.type === 'progress' || data.type === 'completed') {
          this.loadStatus();
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
    if (!this.status.batchesExpected || this.status.batchesExpected === 0) {
      return 0;
    }
    return Math.round((this.status.batchesCompleted / this.status.batchesExpected) * 100);
  }

  get statusClass(): string {
    return `status-${this.status.status?.toLowerCase() || 'running'}`;
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

