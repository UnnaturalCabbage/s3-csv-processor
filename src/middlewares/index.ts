import { NextFunction, Request, Response } from "express";
import { validationResult } from "express-validator";

export function notFound(req: Request, res: Response, next: NextFunction) {
  res.status(404);
  const error = new Error(`🔍 - Not Found - ${req.originalUrl}`);
  next(error);
}

export function errorHandler(
  err: Error,
  req: Request,
  res: Response,
  next: NextFunction
) {
  const statusCode = res.statusCode !== 200 ? res.statusCode : 500;
  res.status(statusCode);
  res.json({
    message: err.message,
  });
}

export function validationHandler(
  req: Request,
  res: Response,
  next: NextFunction
) {
  const result = validationResult(req);
  if (result.isEmpty()) next();
  else
    res.send({
      error: result.array(),
    });
}
