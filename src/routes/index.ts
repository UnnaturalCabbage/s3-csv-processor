import express from "express";
import * as expenseController from "../controllers/expenseController";
import { body, query } from "express-validator";
import { validationHandler } from "../middlewares";

const router = express.Router();

router.get(
  "/expense",
  query("id").notEmpty(),
  validationHandler,
  expenseController.getExpenseById
);
router.get(
  "/expense/report",
  query("companyId").notEmpty(),
  query("reportId").notEmpty(),
  validationHandler,
  expenseController.getExpensesReport
);
router.post(
  "/expense/upload",
  body("url").isURL().notEmpty(),
  validationHandler,
  expenseController.uploadFromS3CSV
);
router.get(
  "/expense/summary",
  query("summaryId").notEmpty(),
  validationHandler,
  expenseController.getUploadSummary
);

export default router;
