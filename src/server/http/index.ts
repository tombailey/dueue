import express from "express"
import morgan from "morgan"
import HealthRouter from "./router/health"
import NotFoundRouter from "./router/notFound"
import DueueRouter from "./router/dueue"
import DueueController from "../../controller/dueue"
import handleError from "./middleware/error"

export async function createHttpServer(dueueController: DueueController) {
  const expressApp = express();
  expressApp.disable("x-powered-by");
  expressApp.use(morgan("combined"));
  expressApp.use(express.json());

  new HealthRouter().registerRoutes(expressApp);
  new DueueRouter(dueueController).registerRoutes(expressApp);
  new NotFoundRouter().registerRoutes(expressApp);

  expressApp.use(handleError);

  return expressApp;
}
