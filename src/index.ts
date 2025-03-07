import { createClient } from "redis";
import config from "config";
import { MongoClient } from "mongodb";
import app from "./app";
import Logger from "./common/logger/Logger";
import ExpenseService from "./services/ExpenseService";
import { ReportModel } from "./models/ReportModel";
import { ExpenseModel } from "./models/ExpenseModel";
import { UploadSummaryModel } from "./models/UploadSummaryModel";

export const logger = new Logger();

export const redisClient = createClient({
  url: config.get<string | undefined>("redis.url"),
});

const mongoClient = new MongoClient(config.get<string>("mongo.url"));
const serviceDbClient = mongoClient.db(config.get<string>("mongo.database"));

// TODO: refactor and move mongo related code to another file
export const reportsColClient =
  serviceDbClient.collection<ReportModel>("reports");
export const expensesColClient =
  serviceDbClient.collection<ExpenseModel>("expenses");
export const summariesColClient =
  serviceDbClient.collection<UploadSummaryModel>("summaries");

export const expenseService = new ExpenseService();

(async () => {
  await Promise.all([redisClient.connect(), mongoClient.connect()]);
  expensesColClient.createIndex({
    companyId: 1,
    reportId: 1,
  });
  app(config.get<number>("server.port"));
})();
