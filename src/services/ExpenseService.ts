import { parse } from "csv-parse";
import { v4 as uuidv4 } from "uuid";
import async from "async";
import _ from "lodash";
import {
  expensesColClient,
  redisClient,
  reportsColClient,
  summariesColClient,
} from "..";
import * as s3Utils from "../common/s3/utils";
import { ExpenseModel, ExpenseStatus } from "../models/ExpenseModel";
import { batch } from "../common/stream/utils";
import Queue from "bull";
import { UploadSummaryModel } from "../models/UploadSummaryModel";
import { companiesToExclude } from "../common/constants";
import { ReportModel } from "../models/ReportModel";

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
          done
        );
      }
    );
  }

  getExpenseById(expenseId: string): Promise<ExpenseModel | null> {
    return expensesColClient.findOne<ExpenseModel>({
      expenseId,
    });
  }

  async getExpensesReport(
    companyId: string,
    reportId: string
  ): Promise<ReportModel | null> {
    let report = await reportsColClient.findOne({
      companyId,
      reportId,
    });
    if (report) return new ReportModel(report);

    // fallback to Expenses collection if report was not generated yet
    const expensesCursor = await expensesColClient.find<ExpenseModel>({
      companyId,
      reportId,
    });
    const expenses: ExpenseModel[] = [];
    let companyName = "";
    for await (const expense of expensesCursor) {
      companyName = expense.companyName;
      expenses.push(expense);
    }
    if (expenses.length === 0) return null;
    report = new ReportModel({
      reportId,
      companyId,
      companyName,
      expenses: expenses,
    });
    return report;
  }

  async addUploadExpensesJob(url: string): Promise<string | null> {
    const parsedS3Url = s3Utils.parseObjectUrl(url);
    if (!parsedS3Url) return null;
    const { region, bucket, key } = parsedS3Url;
    const exists = await s3Utils.objectExists(region, bucket, key);
    if (!exists) return null;
    const summaryId = await this.initSummary();
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
      summaryId
    )) as unknown as UploadSummaryModel;
    if ("_id" in summary) {
      summary.totalReports = (
        await redisClient.sCard("reports_" + summaryId)
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
      Downloading from S3 goes much faster than processing of the data,
      so we need to pause stream and wait for tasks to complete
      in order to not run into memory issues
  */
    let currProcessing = 0;
    let streamEnded = false;
    const stream = s3Utils.readObject(region, bucket, key);
    stream
      .pipe(
        parse({
          columns: true,
        })
      )
      .pipe(batch(1000))
      .on("data", async (expenses) => {
        expenses = ExpenseModel.fromArray(expenses);
        currProcessing += 1;
        if (currProcessing >= ExpenseService.maxProcessingPerStream) {
          stream.pause();
        }
        await this.handleExpensesUploadChunk(expenses, summaryId);
        currProcessing -= 1;
        if (streamEnded && currProcessing === 0) {
          await this.handleExpensesUploadEnd(summaryId);
          callback();
        }
        if (currProcessing < ExpenseService.maxProcessingPerStream) {
          stream.resume();
        }
      })
      .on("end", async () => {
        streamEnded = true;
        if (currProcessing === 0) {
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

    await Promise.all([
      // Updating reports dynamically on each chunk takes a while... might be due to free  mongo atlas account rate limits
      // reportsColClient.bulkWrite(
      //   reports.map((r) => {
      //     return {
      //       updateOne: {
      //         filter: {
      //           _id: r._id,
      //         },
      //         update: {
      //           $set: {
      //             reportId: r.reportId,
      //             companyId: r.companyId,
      //             companyName: r.companyName,
      //           },
      //           $push: {
      //             expenses: {
      //               $each: r.expenses,
      //             },
      //           },
      //         },
      //         upsert: true,
      //       },
      //     };
      //   })
      // ),
      expensesColClient.insertMany(expenses),
    ]);

    // Update real-time data
    const statuses = this.countStatuses(expenses);
    const reportKeys = this.getUniqReportKeys(completedExpenses);
    const redisOperations = [];
    redisOperations.push(redisClient.sAdd("reports_" + summaryId, reportKeys));
    if (statuses.completed)
      redisOperations.push(
        redisClient.hIncrBy(summaryId, "totalCompleted", statuses.completed)
      );
    if (statuses.excluded)
      redisOperations.push(
        redisClient.hIncrBy(summaryId, "totalExcluded", statuses.excluded)
      );
    if (statuses.failed)
      redisOperations.push(
        redisClient.hIncrBy(summaryId, "totalFailed", statuses.failed)
      );
    redisOperations.push(redisClient.hSet(summaryId, "status", "processing"));
    await Promise.all(redisOperations);
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

    redisClient.hSet(summaryId, "status", "completed");
    redisClient.expire("reports_" + summaryId, 60);
    redisClient.expire(summaryId, 60);

    // workaround for generatins reports
    const reportKeys = await redisClient.sMembers("reports_" + summaryId);
    const parsedReportKeys = reportKeys.map((r) => r.split("/"));
    const reports: ReportModel[] = [];

    // TODO: use findyMany/aggregations instead
    await async.eachLimit(
      parsedReportKeys,
      50,
      async ([companyId, reportId]) => {
        const report = await this.getExpensesReport(companyId, reportId);
        if (report) {
          reports.push(report);
        }
      }
    );
    reportsColClient.insertMany(reports);
  }

  private async initSummary() {
    const summaryId = uuidv4();
    await redisClient.hSet(summaryId, {
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

  private getUniqReportKeys(completedExpenses: ExpenseModel[]): string[] {
    return _.chain(completedExpenses)
      .groupBy((e) => e.companyId + "/" + e.reportId)
      .keys()
      .uniq()
      .value();
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
