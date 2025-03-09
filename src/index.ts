import config from "config";
import app from "./app";
import ExpenseService from "./services/ExpenseService/ExpenseService";
import setup from "./setup";

export const expenseService = new ExpenseService();

(async () => {
  await setup();

  app(config.get<number>("server.port"));
})();
