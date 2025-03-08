interface GetReportKeyOptions {
  companyId: string;
  reportId: string;
}
export const getReportKey = ({ companyId, reportId }: GetReportKeyOptions) =>
  companyId + "/" + reportId;

export const getRedisReportKey = (opts: GetReportKeyOptions) =>
  "report_" + getReportKey(opts);

export const getRedisSummaryKey = (summaryId: string) => "summary_" + summaryId;
export const getRedisSummaryReportsKey = (summaryId: string) =>
  "summary_reports_" + summaryId;
