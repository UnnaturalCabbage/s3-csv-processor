export type UploadSummaryStatus = "queued" | "processing" | "completed";
export interface UploadSummaryModel {
  _id: string;
  totalCompleted: string;
  totalFailed: string;
  totalExcluded: string;
  totalReports?: string;
  status: UploadSummaryStatus;
}
