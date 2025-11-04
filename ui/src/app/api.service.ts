import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class ApiService {
  private base = '/api';

  constructor(private http: HttpClient) {}

  upload(file: File): Observable<any> {
    const formData = new FormData();
    formData.append('file', file);
    return this.http.post(`${this.base}/jobs`, formData);
  }

  getJob(jobId: string): Observable<any> {
    return this.http.get(`${this.base}/jobs/${jobId}`);
  }

  getJobs(): Observable<any[]> {
    return this.http.get<any[]>(`${this.base}/jobs`);
  }

  downloadResults(jobId: string): Observable<Blob> {
    return this.http.get(`${this.base}/jobs/${jobId}/results`, { responseType: 'blob' });
  }
}

