import express from "express";
import morgan from "morgan";
import helmet from "helmet";
import cors from "cors";

import * as middlewares from "./middlewares";
import router from "./routes";
import { logger } from "./setup";

require("dotenv").config();

const app = express();

app.use(morgan("dev"));
app.use(helmet());
app.use(cors());
app.use(express.json());

app.use("/v1", router);
app.get<{}, string>("/health", (req, res) => {
  res.sendStatus(204);
});
app.use(middlewares.notFound);
app.use(middlewares.errorHandler);

export default (port: number) =>
  app.listen(port, () => {
    logger.log(`Listening: http://localhost:${port}`);
  });
