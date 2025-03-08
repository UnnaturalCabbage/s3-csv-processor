export enum ExpenseStatus {
  Completed = "completed",
  Excluded = "excluded",
  Failed = "failed",
}
export interface ExpenseModel {
  expenseId: string;
  companyId: string;
  reportId: string;
  companyName: string;
  amount: string;
  image: string;
  status?: ExpenseStatus;
}

export interface MinifiedExpense
  extends Omit<ExpenseModel, "reportId" | "companyId" | "companyName"> {}

export const minifyExpense = (
  expense: ExpenseModel | MinifiedExpense
): MinifiedExpense => {
  return {
    expenseId: expense.expenseId,
    amount: expense.amount,
    image: expense.image,
  };
};
