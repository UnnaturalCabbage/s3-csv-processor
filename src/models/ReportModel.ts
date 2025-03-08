import _ from "lodash";
import { ExpenseModel, MinifiedExpense, minifyExpense } from "./ExpenseModel";
import { ObjectId } from "mongodb";

interface ReporModelOptions {
  _id?: string | ObjectId;
  reportId: string;
  companyId: string;
  companyName: string;
  expenses: (ExpenseModel | MinifiedExpense)[];
}

export interface ReportModel {
  reportId: string;
  companyId: string;
  companyName: string;
  expenses: MinifiedExpense[];
}

export const reportsFromExpenses = (expenses: ExpenseModel[]): ReportModel[] => {
  const expensesByReport = _.groupBy(expenses, (e) => e.companyId + e.reportId);
  const reports: ReportModel[] = Object.values(expensesByReport).map(
    (expenseGroup) => ({
      reportId: expenseGroup[0].reportId,
      companyId: expenseGroup[0].companyId,
      companyName: expenseGroup[0].companyName,
      expenses: expenseGroup.map((e) => minifyExpense(e)),
    })
  );

  return reports;
};
