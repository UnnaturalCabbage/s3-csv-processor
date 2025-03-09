import { v4 as uuidv4 } from "uuid";
import config from "config";
import { Queue, Worker } from "bullmq";
import {
  expensesColClient,
  logger,
  redisClient,
  reportsColClient,
  summariesColClient,
} from "../../setup";
import * as s3Utils from "../../common/s3/utils";
import { ExpenseModel, minifyExpense } from "../../models/ExpenseModel";
import { UploadSummaryModel } from "../../models/UploadSummaryModel";
import { ReportModel, reportsFromExpenses } from "../../models/ReportModel";
import {
  getRedisReportKey,
  getRedisSummaryKey,
  getRedisSummaryReportsKey,
} from "./utils";
import {
  maxThreads,
  maxConcurrentJobsPerThread,
  uploadExpensesQueueName,
} from "./config";

interface UploadExpenseJob {
  summaryId: string;
  region: string;
  bucket: string;
  key: string;
}

export default class ExpenseService {
  private static uploadExpensesQueue = new Queue<UploadExpenseJob>(
    uploadExpensesQueueName,
    {
      connection: {
        host: config.get("redis.host"),
        port: config.get("redis.port"),
      },
    }
  );

  constructor() {
    for (let i = 0; i < maxThreads; i += 1) {
      new Worker(
        uploadExpensesQueueName,
        `${__dirname}/uploadExpensesWorker.js`,
        {
          useWorkerThreads: true,
          concurrency: maxConcurrentJobsPerThread,
          connection: {
            host: config.get("redis.host"),
            port: config.get("redis.port"),
          },
        }
      );
    }
  }

  getExpenseById(expenseId: string): Promise<ExpenseModel | null> {
    return expensesColClient.findOne<ExpenseModel>({ expenseId });
  }

  async getExpensesReport(
    companyId: string,
    reportId: string
  ): Promise<ReportModel | null> {
    let report: ReportModel | null = (await redisClient.json.get(
      getRedisReportKey({ companyId, reportId })
    )) as ReportModel | null;
    if (report) return report;

    report = await reportsColClient.findOne({ companyId, reportId });
    if (report) return report;

    // Fallback to generating the report on the fly from the expenses collection.
    const expensesCursor = await expensesColClient.find<ExpenseModel>({
      companyId,
      reportId,
    });
    const expenses: ExpenseModel[] = [];
    for await (const expense of expensesCursor) {
      expenses.push(expense);
    }
    if (expenses.length === 0) return null;
    report = reportsFromExpenses(expenses)[0];
    return report;
  }

  async generateExpenseReports(
    data: { companyId: string; reportId: string }[]
  ): Promise<void> {
    const expensesCursor = await expensesColClient.find<ExpenseModel>({
      $or: data,
    });
    const reports = new Map<string, ReportModel>();
    for await (const expense of expensesCursor) {
      const { companyId, reportId, companyName } = expense;
      const minifiedExpense = minifyExpense(expense);
      const reportKey = companyId + reportId;
      let report = reports.get(reportKey);
      if (!report) {
        report = {
          companyId,
          reportId,
          companyName,
          expenses: [minifiedExpense],
        };
        reports.set(reportKey, report);
      } else {
        report.expenses.push(minifiedExpense);
      }
    }
    await reportsColClient.insertMany([...reports.values()], {
      ordered: false,
    });
  }

  async addUploadExpensesJob(url: string): Promise<string | null> {
    const parsedS3Url = s3Utils.parseObjectUrl(url);
    if (!parsedS3Url) return null;
    const { region, bucket, key } = parsedS3Url;
    const exists = await s3Utils.objectExists(region, bucket, key);
    if (!exists) return null;
    const summaryId = await this.initSummary();
    logger.log(`Add upload expenses job with id ${summaryId}`);
    await ExpenseService.uploadExpensesQueue.add("upload", {
      summaryId,
      region,
      bucket,
      key,
    });
    return summaryId;
  }

  async getUploadSummary(
    summaryId: string
  ): Promise<UploadSummaryModel | null> {
    let summary: UploadSummaryModel | null = (await redisClient.hGetAll(
      getRedisSummaryKey(summaryId)
    )) as unknown as UploadSummaryModel;
    if ("summaryId" in summary) {
      summary.totalReports = (
        await redisClient.sCard(getRedisSummaryReportsKey(summaryId))
      ).toString();
    } else {
      summary = await summariesColClient.findOne({
        summaryId,
      });
    }
    return summary;
  }

  private async initSummary(): Promise<string> {
    const summaryId = uuidv4();
    await redisClient.hSet(getRedisSummaryKey(summaryId), {
      summaryId,
      totalCompleted: 0,
      totalExcluded: 0,
      totalFailed: 0,
      status: "queued",
    });
    return summaryId;
  }
}
