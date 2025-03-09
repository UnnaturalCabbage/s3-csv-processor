import config from "config";
import { MongoClient } from "mongodb";
import { ReportModel } from "./models/ReportModel";
import { ExpenseModel } from "./models/ExpenseModel";
import { UploadSummaryModel } from "./models/UploadSummaryModel";
import Logger from "./common/logger/Logger";
import { createClient } from "redis";

export const logger = new Logger();

export const redisClient = createClient({
  url: config.get("redis.url"),
});

const mongoClient = new MongoClient(config.get<string>("mongo.url"));
const serviceDbClient = mongoClient.db(config.get<string>("mongo.database"));

export const reportsColClient =
  serviceDbClient.collection<ReportModel>("reports");
export const expensesColClient =
  serviceDbClient.collection<ExpenseModel>("expenses");
export const summariesColClient =
  serviceDbClient.collection<UploadSummaryModel>("summaries");

export default async function setup() {
  await Promise.all([mongoClient.connect(), redisClient.connect()]);
  await Promise.all([
    expensesColClient.createIndex({
      expenseId: 1,
    }),
    expensesColClient.createIndex({
      companyId: 1,
      reportId: 1,
    }),
    reportsColClient.createIndex({
      companyId: 1,
      reportId: 1,
    }),
    summariesColClient.createIndex({
      summaryId: 1,
    }),
  ]);
}
