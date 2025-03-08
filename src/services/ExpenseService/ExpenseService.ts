import { parse } from "csv-parse";
import { v4 as uuidv4 } from "uuid";
import async from "async";
import _ from "lodash";
import {
  expensesColClient,
  logger,
  redisClient,
  reportsColClient,
  summariesColClient,
} from "../..";
import * as s3Utils from "../../common/s3/utils";
import {
  ExpenseModel,
  ExpenseStatus,
  minifyExpense,
} from "../../models/ExpenseModel";
import { batch } from "../../common/stream/utils";
import Queue from "bull";
import { UploadSummaryModel } from "../../models/UploadSummaryModel";
import { companiesToExclude } from "../../common/constants";
import { ReportModel, reportsFromExpenses } from "../../models/ReportModel";
import {
  getRedisReportKey,
  getRedisSummaryKey,
  getRedisSummaryReportsKey,
  getReportKey,
} from "./utils";

interface UploadExpenseJob {
  summaryId: string;
  region: string;
  bucket: string;
  key: string;
}

export default class ExpenseService {
  /*
    with such configuration worst possible scenario of memory usage is super roughly:

    maxProcessingPerStream * maxConcurrentJobs * chunkSize(Megabytes) = n(Megabytes)
    In current setup - 100 * 3 * 2 = 500Mb.

    Values can be adjusted accordingly to needs and machine capabilities,
    tho Mongo, s3 or any other service rate-limits need to be counted too
  */
  private static maxProcessingPerStream = 100;
  private static maxConcurrentJobs = 3;

  private static uploadExpensesQueue = new Queue<UploadExpenseJob>(
    "upload-expenses-queue"
  ); // simple queue manager based on redis, can be replaced with sqs or any other one

  constructor() {
    ExpenseService.uploadExpensesQueue.process(
      ExpenseService.maxConcurrentJobs,
      (job, done) => {
        this.uploadExpensesFromS3CSV(
          job.data.summaryId,
          job.data.region,
          job.data.bucket,
          job.data.key,
          () => {
            logger.log(
              `Finished upload expesnes job with id ${job.data.summaryId}`
            );
            done();
          }
        );
      }
    );
  }

  getExpenseById(expenseId: string): Promise<ExpenseModel | null> {
    return expensesColClient.findOne<ExpenseModel>({
      expenseId: expenseId,
    });
  }

  async getExpensesReport(
    companyId: string,
    reportId: string
  ): Promise<ReportModel | null> {
    let report: ReportModel | null = (await redisClient.json.get(
      getRedisReportKey({ companyId, reportId })
    )) as ReportModel | null;

    if (report) return report;

    report = await reportsColClient.findOne({
      companyId,
      reportId,
    });
    if (report) return report;

    // fallback to Expenses collection if report was not generated yet or deleted from redis
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
    logger.log(`Add upload expesnes job with id ${summaryId}`);
    ExpenseService.uploadExpensesQueue.add({
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
    if ("_id" in summary) {
      summary.totalReports = (
        await redisClient.sCard(getRedisSummaryReportsKey(summaryId))
      ).toString();
    } else {
      summary = await summariesColClient.findOne({
        _id: summaryId,
      });
    }
    return summary;
  }

  /*
  TODO: create and send a job to a queue rather than proccessing file directly
  TODO: calculate and limit amount of parallel running jobs
  */
  uploadExpensesFromS3CSV(
    summaryId: string,
    region: string,
    bucket: string,
    key: string,
    callback: () => void
  ) {
    /*
      The downloading from S3 goes much faster than processing of the data,
      so we need to pause stream and wait for tasks to complete
      in order to not run into memory issues
  */
    let currProcessing = 0;
    let streamEnded = false;
    const stream = s3Utils.readObject(region, bucket, key);
    logger.log(
      `Started uploading expenses from object ${region} ${bucket} ${key}`
    );
    let tmp = 0;
    stream
      .pipe(
        parse({
          columns: true,
        })
      )
      .pipe(batch(1000))
      .on("data", async (expenses) => {
        currProcessing += 1;
        tmp += 1;
        if (currProcessing >= ExpenseService.maxProcessingPerStream) {
          stream.pause();
        }
        try {
          if (tmp < 50) {
            await this.handleExpensesUploadChunk(expenses, summaryId);
          }
          if (streamEnded && currProcessing === 1) {
            await this.handleExpensesUploadEnd(summaryId);
            callback();
          }
        } catch {}
        currProcessing -= 1;
        if (currProcessing < ExpenseService.maxProcessingPerStream) {
          stream.resume();
        }
      })
      .on("end", async () => {
        streamEnded = true;
        if (currProcessing === 0) {
          logger.log(`End`);
          await this.handleExpensesUploadEnd(summaryId);
          callback();
        }
      });
    return summaryId;
  }

  private async handleExpensesUploadChunk(
    expenses: ExpenseModel[],
    summaryId: string
  ) {
    await this.classifyExpenses(expenses);
    const completedExpenses = expenses.filter(
      (e) => e.status === ExpenseStatus.Completed
    );

    const reports = reportsFromExpenses(completedExpenses);

    await Promise.all([
      expensesColClient.insertMany(expenses),
      (async () => {
        // Update real-time data
        const statuses = this.countStatuses(expenses);

        let redisMulti = redisClient.multi();
        for (const report of reports) {
          const redisKey = getRedisReportKey(report);
          redisMulti.exists(redisKey);
        }
        const existingReports = await redisMulti.execAsPipeline();

        redisMulti = redisClient.multi();
        reports.forEach((report, idx) => {
          const redisKey = getRedisReportKey(report);
          const exists = existingReports[idx] === 1;
          /* 
              TODO: There is a chance that the report may be created right after EXISTS executed by any other process, 
              so next command (JSON.SET) might replace existing report. It can be fixed with custom redis scripts
          */
          if (exists) {
            redisClient.json.arrAppend(
              redisKey,
              "$.expenses",
              ...(report.expenses as any)
            );
          } else {
            redisMulti.json.set(redisKey, "$", report as any);
          }
        });

        const reportKeys = reports.map((r) => getReportKey(r));
        redisMulti.sAdd(getRedisSummaryReportsKey(summaryId), reportKeys);
        if (statuses.completed)
          redisMulti.hIncrBy(
            getRedisSummaryKey(summaryId),
            "totalCompleted",
            statuses.completed
          );
        if (statuses.excluded)
          redisMulti.hIncrBy(
            getRedisSummaryKey(summaryId),
            "totalExcluded",
            statuses.excluded
          );
        if (statuses.failed)
          redisMulti.hIncrBy(
            getRedisSummaryKey(summaryId),
            "totalFailed",
            statuses.failed
          );
        redisMulti.hSet(getRedisSummaryKey(summaryId), "status", "processing");
        await redisMulti.execAsPipeline();
      })(),
    ]);
  }

  private async handleExpensesUploadEnd(summaryId: string) {
    const summary = await this.getUploadSummary(summaryId);
    await summariesColClient.updateOne(
      {
        summaryId,
      },
      {
        $set: {
          ...summary!,
          status: "completed",
        },
      },
      {
        upsert: true,
      }
    );

    const reportKeys = await redisClient.sMembers(
      getRedisSummaryReportsKey(summaryId)
    );
    const parsedReportKeys = reportKeys.map((r) => {
      const [companyId, reportId] = r.split("/");
      return {
        companyId,
        reportId,
      };
    });

    await Promise.all([
      this.generateExpenseReports(parsedReportKeys),
      redisClient
        .multi()
        .hSet(getRedisSummaryKey(summaryId), "status", "completed")
        .expire(getRedisSummaryReportsKey(summaryId), 60)
        .expire(getRedisSummaryKey(summaryId), 60)
        .execAsPipeline(),
    ]);
  }

  private async initSummary() {
    const summaryId = uuidv4();
    await redisClient.hSet(getRedisSummaryKey(summaryId), {
      _id: summaryId,
      totalCompleted: 0,
      totalExcluded: 0,
      totalFailed: 0,
      status: "queued",
    });
    return summaryId;
  }

  // modify expenses directly to safe memory usage
  private async classifyExpenses(expenses: ExpenseModel[]): Promise<void> {
    await async.eachLimit(expenses, 100, async (expense: ExpenseModel) => {
      const status = await this.getExpenseStatus(expense);
      expense.status = status;
      return expense;
    });
  }

  private countStatuses(classifiedExpenses: ExpenseModel[]) {
    return _.countBy(classifiedExpenses, (e) => e.status) as Record<
      ExpenseStatus,
      number
    >;
  }

  private async getExpenseStatus(
    expense: ExpenseModel
  ): Promise<ExpenseStatus> {
    if (companiesToExclude.includes(expense.companyId))
      return ExpenseStatus.Excluded;
    if (!expense.amount || expense.amount === "1") return ExpenseStatus.Failed;
    return ExpenseStatus.Completed;
  }
}
