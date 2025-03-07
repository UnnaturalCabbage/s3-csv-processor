import { Request, Response } from "express";
import { ExpenseModel } from "../models/ExpenseModel";
import { expenseService } from "..";
import { UploadSummaryModel } from "../models/UploadSummaryModel";
import { ReportModel } from "../models/ReportModel";

type GetExpenseQuery = {
  id: string;
};
type GetExpenseResponse = {
  data: ExpenseModel | null;
};
export const getExpenseById = async (
  req: Request<{}, {}, {}, GetExpenseQuery>,
  res: Response<GetExpenseResponse>
) => {
  const expense = await expenseService.getExpenseById(req.query.id);
  if (!expense) {
    res.json({
      data: null,
    });
  } else {
    res.json({
      data: expense,
    });
  }
};

type GetExpensesReportQuery = {
  reportId: string;
  companyId: string;
};
type GetExpensesReporResponse = {
  data: ReportModel | null;
};
export const getExpensesReport = async (
  req: Request<{}, {}, {}, GetExpensesReportQuery>,
  res: Response<GetExpensesReporResponse>
) => {
  const { reportId, companyId } = req.query;
  const report = await expenseService.getExpensesReport(companyId, reportId);
  if (!report) {
    res.json({
      data: null,
    });
  } else {
    res.json({
      data: report,
    });
  }
};

type UploadFromS3CSVBody = {
  url: string;
};
type PostUploadFromS3CSVResponse = {
  data: {
    summaryId: string;
  } | null;
};
export const uploadFromS3CSV = async (
  req: Request<{}, {}, UploadFromS3CSVBody>,
  res: Response<PostUploadFromS3CSVResponse>
) => {
  const summaryId = await expenseService.addUploadExpensesJob(req.body.url);
  if (!summaryId) {
    res.json({
      data: null,
    });
  } else {
    res.json({
      data: { summaryId: summaryId },
    });
  }
};

type UploadSummaryQuery = {
  summaryId: string;
};
type GetUploadSummaryResponse = {
  data: UploadSummaryModel | null;
};
export const getUploadSummary = async (
  req: Request<{}, {}, {}, UploadSummaryQuery>,
  res: Response<GetUploadSummaryResponse>
) => {
  const { summaryId } = req.query;
  const summary = await expenseService.getUploadSummary(summaryId);
  if (!summary) {
    res.json({
      data: null,
    });
  } else {
    res.json({
      data: summary,
    });
  }
};
