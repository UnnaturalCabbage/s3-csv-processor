import _ from "lodash";
import { ExpenseModel, MinifiedExpense } from "./ExpenseModel";

interface ReporModelOptions {
  reportId: string;
  companyId: string;
  companyName: string;
  expenses: (ExpenseModel | MinifiedExpense)[];
}

export class ReportModel {
  _id: string;
  reportId: string;
  companyId: string;
  companyName: string;
  expenses: (MinifiedExpense | MinifiedExpense)[];

  constructor({
    reportId,
    companyId,
    companyName,
    expenses,
  }: ReporModelOptions) {
    this._id = `${companyId}-${reportId}`; // assume those fields combination is always uniq
    this.reportId = reportId;
    this.companyId = companyId;
    this.companyName = companyName;
    this.expenses = expenses.map((e) => ExpenseModel.toMinified(e));
  }

  static fromExpenses(expenses: ExpenseModel[]) {
    const expensesByReport = _.groupBy(
      expenses,
      (e) => e.companyId + e.reportId
    );
    const reports: ReportModel[] = Object.values(expensesByReport).map(
      (expenseGroup) =>
        new ReportModel({
          reportId: expenseGroup[0].reportId,
          companyId: expenseGroup[0].companyId,
          companyName: expenseGroup[0].companyName,
          expenses: expenseGroup,
        })
    );

    return reports;
  }
}
