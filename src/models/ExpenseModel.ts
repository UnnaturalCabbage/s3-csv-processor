export enum ExpenseStatus {
  Completed = "completed",
  Excluded = "excluded",
  Failed = "failed",
}
interface ExpenseModelOptions {
  expenseId: string;
  companyId: string;
  reportId: string;
  companyName: string;
  amount: string;
  image: string;
  status?: ExpenseStatus;
}
export class ExpenseModel {
  _id: string;
  expenseId: string;
  companyId: string;
  reportId: string;
  companyName: string;
  amount: string;
  image: string;
  status?: ExpenseStatus;

  constructor(options: ExpenseModelOptions) {
    this._id = options.expenseId;
    this.expenseId = options.expenseId;
    this.companyId = options.companyId;
    this.reportId = options.reportId;
    this.companyName = options.companyName;
    this.amount = options.amount;
    this.image = options.image;
  }

  static fromArray(expensesData: ExpenseModelOptions[]) {
    return expensesData.map((e) => new ExpenseModel(e));
  }

  static toMinified(expense: ExpenseModel | MinifiedExpense): MinifiedExpense {
    return {
      _id: expense._id,
      expenseId: expense.expenseId,
      amount: expense.amount,
      image: expense.image,
    };
  }
}

export interface MinifiedExpense
  extends Omit<ExpenseModel, "reportId" | "companyId" | "companyName"> {}
