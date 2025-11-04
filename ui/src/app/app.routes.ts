import { Routes } from '@angular/router';
import { UploadComponent } from './upload/upload.component';
import { JobStatusComponent } from './job-status/job-status.component';

export const routes: Routes = [
  { path: '', component: UploadComponent },
  { path: 'jobs/:id', component: JobStatusComponent },
];

