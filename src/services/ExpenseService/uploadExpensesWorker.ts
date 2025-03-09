import { parse } from "csv-parse";
import config from "config";
import async from "async";
import _ from "lodash";
import { Job } from "bullmq";
import setup, {
  expensesColClient,
  logger,
  redisClient,
  reportsColClient,
  summariesColClient,
} from "../../setup";
import * as s3Utils from "../../common/s3/utils";
import { batch } from "../../common/stream/utils";
import { ExpenseModel, ExpenseStatus } from "../../models/ExpenseModel";
import { companiesToExclude } from "../../common/constants";
import { ReportModel, reportsFromExpenses } from "../../models/ReportModel";
import {
  getRedisReportKey,
  getRedisSummaryKey,
  getRedisSummaryReportsKey,
  getReportKey,
} from "./utils";
import { maxProcessingPerStream } from "./config";

interface UploadExpenseJob {
  summaryId: string;
  region: string;
  bucket: string;
  key: string;
}

export default async function (job: Job<UploadExpenseJob>) {
  await setup();

  function uploadExpensesFromS3CSV(
    summaryId: string,
    region: string,
    bucket: string,
    key: string,
    onProgress: (v: any) => void,
    onEnd: () => void
  ): string {
    let currProcessing = 0;
    let streamEnded = false;
    const stream = s3Utils.readObject(region, bucket, key);
    logger.log(
      `Started uploading expenses from object ${region} ${bucket} ${key}`
    );
    stream
      .pipe(
        parse({
          columns: true,
        })
      )
      .pipe(batch(2000))
      .on("data", async (expenses: ExpenseModel[]) => {
        currProcessing += 1;
        if (currProcessing >= maxProcessingPerStream) {
          stream.pause();
        }
        try {
          await handleExpensesUploadChunk(expenses, summaryId);
          onProgress({});
          if (streamEnded && currProcessing === 1) {
            await handleExpensesUploadEnd(summaryId);
            onEnd();
          }
        } catch (err) {
          console.error(err);
        }
        currProcessing -= 1;
        if (currProcessing < maxProcessingPerStream) {
          stream.resume();
        }
      })
      .on("end", async () => {
        streamEnded = true;
        if (currProcessing === 0) {
          await handleExpensesUploadEnd(summaryId);
          onEnd();
        }
      });
    return summaryId;
  }

  async function handleExpensesUploadChunk(
    expenses: ExpenseModel[],
    summaryId: string
  ) {
    await classifyExpenses(expenses);
    const completedExpenses = expenses.filter(
      (e) => e.status === ExpenseStatus.Completed
    );
    await Promise.all([
      expensesColClient.insertMany(expenses),
      (async () => {
        const reports = reportsFromExpenses(completedExpenses);
        const statuses = countStatuses(expenses);
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

  async function handleExpensesUploadEnd(summaryId: string) {
    const summary = await redisClient.hGetAll(getRedisSummaryKey(summaryId));
    await summariesColClient.updateOne(
      { summaryId },
      {
        $set: {
          ...summary,
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
      return { companyId, reportId };
    });

    await Promise.all([
      generateExpenseReports(parsedReportKeys),
      redisClient
        .multi()
        .hSet(getRedisSummaryKey(summaryId), "status", "completed")
        .expire(getRedisSummaryReportsKey(summaryId), 60)
        .expire(getRedisSummaryKey(summaryId), 60)
        .execAsPipeline(),
    ]);
  }

  async function generateExpenseReports(
    data: { companyId: string; reportId: string }[]
  ): Promise<void> {
    const expensesCursor = await expensesColClient.find<ExpenseModel>({
      $or: data,
    });
    const reports = new Map<string, ReportModel>();
    for await (const expense of expensesCursor) {
      const { companyId, reportId, companyName } = expense;
      const minifiedExpense = expense;
      const reportKey = companyId + reportId;
      if (!reports.has(reportKey)) {
        reports.set(reportKey, {
          companyId,
          reportId,
          companyName,
          expenses: [minifiedExpense],
        });
      } else {
        reports.get(reportKey)!.expenses.push(minifiedExpense);
      }
    }
    await reportsColClient.insertMany([...reports.values()], {
      ordered: false,
    });
  }

  async function classifyExpenses(expenses: ExpenseModel[]): Promise<void> {
    await async.eachLimit(expenses, 100, async (expense: ExpenseModel) => {
      const status = await getExpenseStatus(expense);
      expense.status = status;
    });
  }

  function countStatuses(classifiedExpenses: ExpenseModel[]) {
    return _.countBy(classifiedExpenses, (e) => e.status) as Record<
      ExpenseStatus,
      number
    >;
  }

  async function getExpenseStatus(
    expense: ExpenseModel
  ): Promise<ExpenseStatus> {
    if (companiesToExclude.includes(expense.companyId))
      return ExpenseStatus.Excluded;
    if (!expense.amount || !expense.image || expense.amount === "1")
      return ExpenseStatus.Failed;
    return ExpenseStatus.Completed;
  }

  return new Promise<void>((resolve) => {
    uploadExpensesFromS3CSV(
      job.data.summaryId,
      job.data.region,
      job.data.bucket,
      job.data.key,
      (v) => {
        job.updateProgress(v);
      },
      () => {
        logger.log(
          `Finished upload expenses job with id ${job.data.summaryId}`
        );
        resolve();
      }
    );
  }).then();
}
